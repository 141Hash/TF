#!/usr/bin/env python3

import logging
from concurrent.futures import ThreadPoolExecutor
from os import stat
import sched
import time
import threading
import random

from ms import send, receiveAll, reply, exitOnError
import math

import logging

class State():
    def __init__(self,node_ids):
        self.node_ids = node_ids
        self.currentState = 0 #0 - follower, 1-candidate, 2-leader
        self.currentTerm = 0
        self.voteCount = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = -1
        self.lastApplied = -1
        self.nextIndex = {}
        self.matchIndex = {}
        self.timeoutSched = None
        self.isHeartBeat = False
        self.isTimeout = False
        self.isDecreaseTimer = False

    def appendLog(self,msg):
        self.log.append((msg,self.currentTerm))

    def appendLogs(self,logs):
        self.log.extend(logs)

    def biggestMatch(self):
        quantities = {}
        for value in self.matchIndex.values():
            quantities.setdefault(value,0)
            quantities[value] += 1
        reversed_list = sorted(quantities.items(),key=lambda x:-x[0])
        majority = math.floor(len(self.matchIndex.keys())/2)+1
        sum = 0
        for ele in reversed_list:
            sum += ele[1]
            if sum >= majority:
                return ele[0]

    def printCurrentState(self):
        logging.info(f"currentTerm=>{self.currentTerm}")
        logging.info(f"log=>{self.log}")
        logging.info(f"commitIndex=>{self.commitIndex}")
        logging.info(f"lastApplied=>{self.lastApplied}")
        logging.info(f"nextIndex=>{self.nextIndex}")
        logging.info(f"matchIndex=>{self.matchIndex}")

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dic = dict()
TIMEOUT_TIME = 2
clock = TIMEOUT_TIME

def heartbeat():
    global state
    while state.isHeartBeat:
        for node in node_ids:
            if node != node_id and state.isHeartBeat:
                try:
                    send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=node_id,
                    prevLogIndex=state.nextIndex[node]-1,
                    prevLogTerm=state.log[state.nextIndex[node]-1][1] if state.nextIndex[node]-1 >= 0 else -1,
                    entries=[],leaderCommit=state.commitIndex)
                except:
                    state.isHeartBeat = False
        time.sleep(0.5)
     
def schedTimeout():
    global state
    if not state.isTimeout:
        state.isTimeout = True
        state.timeoutSched.enter(random.uniform(1,2),1,schedTimeout)
    else:
        logging.info("TIMEOUT")
        state.currentState = 1
        state.currentTerm += 1
        state.votedFor = node_id
        state.voteCount = 1
        for node in node_ids:
            if node != node_id:
                send(node_id,node,type="RRPC",term=state.currentTerm,candidateId=node_id,
                        lastLogIndex=len(state.log)-1,
                        lastLogTerm=-1 if len(state.log) == 0 else state.log[len(state.log)-1][1])        
        

def printBody(msg):
    logging.info("*"*10)
    for k,v in msg.body.__dict__.items():
        logging.info(f"{k}=>{v}")
    logging.info("*"*10)


def apply(msg):
    if msg.body.type == "write":
        dic[msg.body.key] = msg.body.value
    elif msg.body.type == "cas":
        if msg.body.key in dic and getattr(msg.body,"from") == dic[msg.body.key]:
            dic[msg.body.key] = msg.body.to

def reply_cliente(msg):
    if msg.body.type == "write":
        dic[msg.body.key] = msg.body.value
        reply(msg,type="write_ok")
    elif msg.body.type == "cas":
        if msg.body.key not in dic:
            reply(msg,type="error",code=20,text="not found")
        elif getattr(msg.body,"from") != dic[msg.body.key]:
            reply(msg,type="error",code=22,text="does not match")
        else:
            dic[msg.body.key] = msg.body.to
            reply(msg,type="cas_ok")

    elif msg.body.type == "read":
        if msg.body.key not in dic:
            reply(msg,type="error",code=20,text="not found")
        else:
            reply(msg,type="read_ok",value=dic[msg.body.key])


def handle(msg):
    #State
    global node_id, node_ids, state, leader, dic, clock
    #Message Handlers
    
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)
        state = State(node_ids)

        state.isTimeout = True
        state.timeoutSched = sched.scheduler(time.time,time.sleep)
        state.timeoutSched.enter(random.uniform(1,2),1,schedTimeout)
        threading.Thread(target=state.timeoutSched.run).start()

        reply(msg,type='init_ok')
        
    elif state.currentState == 2 and msg.src not in node_ids:
        printBody(msg)
        state.appendLog(msg)
        state.nextIndex[node_id] = len(state.log)
        state.matchIndex[node_id] = len(state.log)-1
        for node in node_ids:
            if node != node_id:
                #If last log index ≥ nextIndex for a follower: send
                if len(state.log)-1 >= state.nextIndex[node]:
                    send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=node_id,
                    prevLogIndex=state.nextIndex[node]-1,prevLogTerm=state.log[state.nextIndex[node]-1][1],
                    entries=state.log[state.nextIndex[node]:],leaderCommit=state.commitIndex)
        logging.info("RECEIVED")
        

    elif state.currentState != 2 and msg.src not in node_ids:
        reply(msg,type="error",code=11,text="not leader")

    elif msg.body.type == "ARPC":
        # printBody(msg)
        state.isTimeout = False

        #If AppendEntries RPC received from new leader: convert to follower
        if state.currentState == 1:
            state.currentState = 0
            # Recomeça timeout timer
            state.isHeartBeat = False
            state.isTimeout = True
            state.timeoutSched = sched.scheduler(time.time,time.sleep)
            state.timeoutSched.enter(random.uniform(1,2),1,schedTimeout)
            threading.Thread(target=state.timeoutSched.run).start()
            state.votedFor = None

        #If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if msg.body.term > state.currentTerm:
            state.currentTerm = msg.body.term
            state.currentState = 0
            # Recomeça timeout timer
            state.isHeartBeat = False
            state.isTimeout = True
            state.timeoutSched = sched.scheduler(time.time,time.sleep)
            state.timeoutSched.enter(random.uniform(1,2),1,schedTimeout)
            threading.Thread(target=state.timeoutSched.run).start()
            state.votedFor = None

        #Passo 1
        if msg.body.term < state.currentTerm:
            reply(msg,type="ARPC_RESP",term=state.currentTerm,success=False,inconsistency=False)
            logging.info("FALSE 1")
        #Passo 2
        elif (state.log != [] and msg.body.prevLogIndex >= 0 and
                (msg.body.prevLogIndex > len(state.log)-1 or 
                state.log[msg.body.prevLogIndex][1] != msg.body.prevLogTerm)):
            reply(msg,type="ARPC_RESP",term=state.currentTerm,success=False,inconsistency=True)
            logging.info("FALSE 2")
        else:
            entries = msg.body.entries
            i = 1
            #Passo 3
            for e in entries:
                if msg.body.prevLogIndex + i < len(state.log):
                    if state.log[msg.body.prevLogIndex+i][1] != e[1]:
                        state.log = state.log[0:msg.prevLogIndex+(i-1)]
                        break
                else:
                    break
                i+=1
            #Passo 4
            if len(entries) != 0:
                state.appendLogs(entries)
            #Passo 5
            if msg.body.leaderCommit > state.commitIndex:
                state.commitIndex = min(msg.body.leaderCommit,len(state.log)-1)
                if state.commitIndex > state.lastApplied:
                    state.lastApplied += 1
                    apply(state.log[state.lastApplied][0])
                    logging.info(dic)
            reply(msg,type="ARPC_RESP",nextIndex=len(state.log),matchIndex=len(state.log)-1,
                term=state.currentTerm,success=True)
            # state.printCurrentState()

    elif msg.body.type == "ARPC_RESP":
        #If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if msg.body.term > state.currentTerm:
            # Recomeça timeout timer
            state.isHeartBeat = False
            state.isTimeout = True
            state.timeoutSched = sched.scheduler(time.time,time.sleep)
            state.timeoutSched.enter(random.uniform(1,2),1,schedTimeout)
            threading.Thread(target=state.timeoutSched.run).start()
            state.currentTerm = msg.body.term
            state.currentState = 0
            state.votedFor = None

        elif msg.body.success:
            state.nextIndex[msg.src] = max(msg.body.nextIndex,state.nextIndex[msg.src])
            state.matchIndex[msg.src] = max(msg.body.matchIndex,state.matchIndex[msg.src])
            n = state.biggestMatch()
            if n >= 0 and n<= len(state.log)-1 and n > state.commitIndex and state.log[n][1] == state.currentTerm:
                state.commitIndex = n
                if state.commitIndex > state.lastApplied:
                    state.lastApplied += 1
                    reply_cliente(state.log[state.lastApplied][0])
                    logging.info(dic)
        elif not msg.body.success and msg.body.inconsistency:
            state.nextIndex[msg.src] -= 1

    elif msg.body.type == "RRPC":
        #Reset Clock
        state.isTimeout = False

        #If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if msg.body.term > state.currentTerm:
            state.currentTerm = msg.body.term
            state.currentState = 0
            state.votedFor = None
            state.isHeartBeat = False


        if msg.body.term < state.currentTerm:
            reply(msg,type="RRPC_RESP",term=state.currentTerm,voteGranted=False)
            
        elif (state.votedFor == None or msg.body.candidateId == state.votedFor) and  msg.body.lastLogIndex >= len(state.log)-1:
            state.votedFor = msg.body.candidateId 
            reply(msg,type="RRPC_RESP",term=state.currentTerm,voteGranted=True)

        else:
            reply(msg,type="RRPC_RESP",term=state.currentTerm,voteGranted=False)
            
    elif msg.body.type == "RRPC_RESP":

        #If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if msg.body.term > state.currentTerm:
            state.currentTerm = msg.body.term
            state.currentState = 0
            state.votedFor = None
            state.isHeartBeat = False

        if msg.body.voteGranted:
            state.voteCount += 1

        #If votes received from majority of servers: become leader   
        if state.voteCount >= math.floor(len(node_ids)/2)+1 and state.currentState != 2:
            logging.info("IM THE LEADER")
            state.nextIndex = {i:len(state.log) for i in node_ids}        
            state.matchIndex = {i:-1 for i in node_ids}
            state.currentState = 2
            state.isHeartBeat = True
            state.isTimeout = False
            state.isDecreaseTimer = False
            threading.Thread(target=heartbeat).start()
    else:
        logging.info("Something Wrong")
    
# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())
