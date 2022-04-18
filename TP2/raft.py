#!/usr/bin/env python3

import logging
from concurrent.futures import ThreadPoolExecutor
from os import stat
from re import S
import time
import threading

from numpy import mat
from ms import send, receiveAll, reply, exitOnError
import math

import logging

class State():
    def __init__(self,node_ids):
        self.currentState = 0 #0 - follower, 1-candidate, 2-leader
        self.currentTerm = 1
        self.votedFor = None
        self.log = []
        self.commitIndex = -1
        self.lastApplied = -1
        self.nextIndex = {i:0 for i in node_ids}
        # Se resp com sucesso
        self.matchIndex = {i:-1 for i in node_ids}
        #Se tamanho do log for mais que o matchIndex do followers, inicia AppendEntries

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

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dic = dict()
TIMEOUT_TIME = 2
clock = TIMEOUT_TIME
#PrevLogIndex esta no nextIndex

def decreaseTime():
    global clock
    while True:
        time.sleep(1)
        clock -= 1
        logging.info(clock)


def timeout():
    global clock,state, node_id, node_ids
    while True:
        if clock < 0:
            clock = TIMEOUT_TIME
            state.currentState = 1
            state.currentTerm += 1
            clock = TIMEOUT_TIME
            for node in node_ids:
                if node != node_id:
                    send(node_id,node,type="RRPC",term=state.term,candidateId=node_id,
                         lastLogIndex=len(state.log)-1,
                         lastLogTerm=-1 if len(state.log) == 0 else state.log[len(state.log)-1][1])
                    


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
        #leader = node_ids[0]
        threading.Thread(target=decreaseTime).start()
        threading.Thread(target=timeout).start()
        # if state.currentState == 2:
        #     state.commitIndex = 0
        # for node in node_ids[1:]:
        #     send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=leader,
        #          prevLogIndex=0,prevLogTerm=0,entries=[],leaderCommit=state.commitIndex)
        reply(msg,type='init_ok')
        
    elif state.currentState == 2 and msg.src not in node_ids:
        state.appendLog(msg)
        for node in node_ids:
            if node != leader:
                if len(state.log) >= state.nextIndex[node]:
                   send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=leader,
                   prevLogIndex=state.nextIndex[node],prevLogTerm=state.log[state.nextIndex[node]][1],
                   entries=state.log[state.nextIndex[node]:],leaderCommit=state.commitIndex) 
        

    elif state.currentState != 2 and msg.src not in node_ids:
        reply(msg,type="error",code=11,text="not leader")

    elif state.currentState != 2 and msg.body.type == "ARPC":
        clock = TIMEOUT_TIME
        if msg.body.term < state.currentTerm:
            logging.info(f"ERROR: TERM")
        elif state.log != [] and state.log[msg.body.prevLogIndex-1][1] != msg.body.prevLogTerm:
            logging.info(f"ERROR: OUTRO")
        else: 
            #@TODO passo 3   
            entries = msg.body.entries
            # index = msg.body.prevLogIndex + 1
            #     if 
            # for entry in entries:
            state.appendLogs(entries)
            # logging.info(f"LEADER:{msg.body.leaderCommit}")
            # logging.info(f"ME:{state.commitIndex}")
            # logging.info("-"*10)
            if msg.body.leaderCommit > state.commitIndex:
                state.commitIndex = min(msg.body.leaderCommit,len(state.log))
                if state.commitIndex > state.lastApplied:
                    apply(state.log[state.lastApplied+1][0])
                    state.lastApplied += 1
            reply(msg,type="ARPC_RESP",nextIndex=len(state.log),matchIndex=len(state.log)-1,
                  term=state.currentTerm,success=True)
        logging.info(dic)


    elif state.currentState == 2 and msg.body.type == "ARPC_RESP":
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

    elif state.currentState == 0 and msg.body.type == "RRPC":
        #@TODO Continuar aqui

# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

# Visto que para todos os servidores, incluindo o leader, o commitIndex começa em 0, tal como o lastAplied que tambem começa em 0, entao quando este enviar um AppendEntries RPC aos seguidores, enviará o leaderCommit tambem a 0, visto que é o valor que tem inicialmente, ou seja 0, a condição do ponto 5. (leaderCommit > commitIndex) nunca irá ser verdadeira, ou seja os seguidores nunca irão alterar o commitIndex.
# Da mesma forma