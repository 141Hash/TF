#!/usr/bin/env python3

import logging
from concurrent.futures import ThreadPoolExecutor
from os import stat
from tkinter.messagebox import NO
from ms import send, receiveAll, reply, exitOnError

import logging

class State():
    def __init__(self,node_ids):
        self.currentTerm = 1
        self.votedFor = None
        self.log = []
        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndex = {i:1 for i in node_ids}
        # Se resp com sucesso
        self.matchIndex = {}
        #Se tamanho do log for mais que o matchIndex do followers, inicia AppendEntries

    def appendLog(self,msg):
        self.log.append((msg,self.currentTerm))

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dic = dict()

#PrevLogIndex esta no nextIndex

def handle(msg):
    #State
    global node_id, node_ids, state
    #Message Handlers
    
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)
        leader = node_ids[0]
        state = State()
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
                   prevLogIndex=state.nextIndex[node]-1,prevLogTerm=state.log[state.nextIndex[node]-1][1],
                   entries=state.log[state.nextIndex[node]-1:],leaderCommit=state.commitIndex) 

    elif node_id != leader and msg.src not in node_ids:
        reply(msg,type="error",code=11,text="not leader")

    elif node_id != leader and msg.type == "ARPC":
        if msg.body.term < state.currentTerm:
            print("False")
        elif state.log != [] and state.body[msg.body.prevLogIndex] != msg.body.prevLogTerm:
            print("False")

        
# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())