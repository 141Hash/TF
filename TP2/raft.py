#!/usr/bin/env python3

import imp
import logging
from concurrent.futures import ThreadPoolExecutor
from os import stat
from re import S
from tkinter.messagebox import NO

from numpy import mat
from ms import send, receiveAll, reply, exitOnError
import math

import logging

class State():
    def __init__(self,node_ids):
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

#PrevLogIndex esta no nextIndex

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
    global node_id, node_ids, state, leader, dic
    #Message Handlers
    
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)
        leader = node_ids[0]
        state = State(node_ids)
        # if node_id == leader:
        #     state.commitIndex = 0
        # for node in node_ids[1:]:
        #     send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=leader,
        #          prevLogIndex=0,prevLogTerm=0,entries=[],leaderCommit=state.commitIndex)
        reply(msg,type='init_ok')
        
    elif node_id == leader and msg.src not in node_ids:
        state.appendLog(msg)
        for node in node_ids:
            if node != leader:
                if len(state.log) >= state.nextIndex[node]:
                   send(node_id,node,type="ARPC",term=state.currentTerm, leaderId=leader,
                   prevLogIndex=state.nextIndex[node],prevLogTerm=state.log[state.nextIndex[node]][1],
                   entries=state.log[state.nextIndex[node]:],leaderCommit=state.commitIndex) 
        

    elif node_id != leader and msg.src not in node_ids:
        reply(msg,type="error",code=11,text="not leader")

    elif node_id != leader and msg.body.type == "ARPC":
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


    elif node_id == leader and msg.body.type == "ARPC_RESP":
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


# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())


# Visto que para todos os servidores, incluindo o leader, o commitIndex começa em 0, tal como o lastAplied que tambem começa em 0, entao quando este enviar um AppendEntries RPC aos seguidores, enviará o leaderCommit tambem a 0, visto que é o valor que tem inicialmente, ou seja 0, a condição do ponto 5. (leaderCommit > commitIndex) nunca irá ser verdadeira, ou seja os seguidores nunca irão alterar o commitIndex.
# Da mesma forma