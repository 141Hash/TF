#!/usr/bin/env python3

import logging
from concurrent.futures import ThreadPoolExecutor
import time
import threading

from ms import send, receiveAll, reply, exitOnError
import math

import logging

class State():
    def __init__(self,node_ids):
        self.node_ids = node_ids
        self.currentState = 0 #0 - follower, 1-candidate, 2-leader
        self.currentTerm = 1
        self.voteCount = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = -1
        self.lastApplied = -1
        self.nextIndex = {}
        self.matchIndex = {}
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
        majority = math.floor(len(self.node_ids)/2)+1
        sum = 0
        for ele in reversed_list:
            sum += ele[1]
            if sum >= majority:
                return ele[0]

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dic = dict()
TIMEOUT_TIME = 2
clock = TIMEOUT_TIME
#PrevLogIndex esta no nextIndex

def decreaseTime():
    global clock,state
    clock = TIMEOUT_TIME
    while state.isDecreaseTimer:
        time.sleep(1)
        clock -= 1
        logging.info(clock)


def timeout():
    global clock,state, node_id, node_ids
    while state.isTimeout:
        if clock < 0:
            clock = TIMEOUT_TIME
            state.currentState = 1
            state.currentTerm += 1
            state.votedFor = node_id
            state.voteCount = 1
            for node in node_ids:
                if node != node_id:
                    send(node_id,node,type="RRPC",term=state.currentTerm,candidateId=node_id,
                         lastLogIndex=len(state.log)-1,
                         lastLogTerm=-1 if len(state.log) == 0 else state.log[len(state.log)-1][1])
                    
def heartbeat():
    while state.isHeartBeat:
        for node in node_ids:
            if node != node_id:
                logging.info(state.log)
                logging.info(state.nextIndex)
                send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=node_id,
                prevLogIndex=state.nextIndex[node]-1,
                prevLogTerm=state.log[state.nextIndex[node]-1][1] if state.nextIndex[node]-1 >= 0 else -1,
                entries=[],leaderCommit=state.commitIndex)
        time.sleep(1)
     

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

        state.isDecreaseTimer = True
        state.isTimeout = True
        threading.Thread(target=decreaseTime).start()
        threading.Thread(target=timeout).start()
        reply(msg,type='init_ok')
        
    elif state.currentState == 2 and msg.src not in node_ids:
        logging.info(msg)
        state.appendLog(msg)
        for node in node_ids:
            if node != node_id:
                #If last log index ≥ nextIndex for a follower: send
                if len(state.log)-1 >= state.nextIndex[node]:
                    #@TODO prevLogIndex é -1 ou n ?
                   send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=node_id,
                   prevLogIndex=state.nextIndex[node]-1,prevLogTerm=state.log[state.nextIndex[node]-1][1],
                   entries=state.log[state.nextIndex[node]:],leaderCommit=state.commitIndex) 
        

    elif state.currentState != 2 and msg.src not in node_ids:
        reply(msg,type="error",code=11,text="not leader")

    elif state.currentState != 2 and msg.body.type == "ARPC":
        clock = TIMEOUT_TIME

        #If AppendEntries RPC received from new leader: convert to follower
        if state.currentState == 1:
            state.currentState = 0
            # Recomeça timeout timer
            state.isHeartBeat = False
            state.isDecreaseTimer = True
            state.isTimeout = True
            threading.Thread(target=decreaseTime).start()
            threading.Thread(target=timeout).start()

        #If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if msg.body.term > state.currentTerm:
            state.currentTerm = msg.body.term
            state.currentState = 0
            # Recomeça timeout timer
            state.isHeartBeat = False
            state.isDecreaseTimer = True
            state.isTimeout = True
            threading.Thread(target=decreaseTime).start()
            threading.Thread(target=timeout).start()

        #Passo 1
        if msg.body.term < state.currentTerm:
            reply(msg,type="ARPC_RESP",term=state.currentTerm,success=False)
            logging.info("FALSE")
        #Passo 2
        elif state.log != [] and msg.body.prevLogIndex >= 0 and state.log[msg.body.prevLogIndex][1] != msg.body.prevLogTerm:
            reply(msg,type="ARPC_RESP",term=state.currentTerm,success=False)
            logging.info("FALSE")
        else: 
            entries = msg.body.entries
            i = 1
            #Passo 3
            for e in entries:
                if msg.body.prevLogIndex + i < len(state.log):
                    if state.log[msg.prevLogIndex+i] != e:
                        state.log = state.log[0:msg.prevLogIndex+(i-1)]
                        break
                else:
                    break
                i+=1
            #Passo 4
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


    elif state.currentState == 2 and msg.body.type == "ARPC_RESP":
        if msg.body.term > state.currentTerm:
            # Recomeça timeout timer
            state.isHeartBeat = False
            state.isDecreaseTimer = True
            state.isTimeout = True
            threading.Thread(target=decreaseTime).start()
            threading.Thread(target=timeout).start()
            state.currentTerm = msg.body.term
            state.currentState = 0
        if msg.body.success:
            state.nextIndex[msg.src] = msg.body.nextIndex
            state.matchIndex[msg.src] = msg.body.matchIndex
            n = state.biggestMatch()
            # Falta a ultima condicao
            if n > state.commitIndex:
                state.commitIndex = n
                if state.commitIndex > state.lastApplied:
                        reply_cliente(state.log[state.lastApplied+1][0])
                        state.lastApplied += 1
            logging.info(dic)
        else:
            state.nextIndex[msg.src] -= 1

    elif state.currentState == 0 and msg.body.type == "RRPC":
        #Reset Clock
        clock = TIMEOUT_TIME

        if msg.body.term > state.currentTerm:
            state.currentTerm = msg.body.term
            state.currentState = 0

        if msg.body.term < state.currentTerm:
            reply(msg,type="RRPC_RESP",term=state.currentTerm,voteGranted=False)
        elif (state.votedFor == None or msg.body.candidateId == state.votedFor) and  msg.body.lastLogIndex >= len(state.log)-1:
            state.votedFor = msg.body.candidateId 
            reply(msg,type="RRPC_RESP",term=state.currentTerm,voteGranted=True)
        else:
            reply(msg,type="RRPC_RESP",term=state.currentTerm,voteGranted=False)
            
    elif state.currentState == 1 and msg.body.type == "RRPC_RESP":
        if msg.body.term > state.currentTerm:
            state.currentTerm = msg.body.term
            state.currentState = 0
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
                    
    
# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

#All Servers: Done
#Followers: Done, acho
#