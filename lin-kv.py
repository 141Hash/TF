#!/usr/bin/env python3

# Simple 'echo' workload in Python for Maelstrom

from asyncore import write
from cmath import log
from distutils.log import info
import imp
import logging
from concurrent.futures import ThreadPoolExecutor
from operator import itemgetter
from types import SimpleNamespace
from numpy import maximum

from scipy import rand
from sqlalchemy import false
from ms import send, receiveAll, reply, exitOnError

import random
import logging
import math

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dic = dict()

#Mandar pedido para todos os quorums
#Esperar pela resposta de todos
#Se ainda estiver à espera mandar pedidos para queue
#Quando receber todas as respostas, pegar ultimo pedido da queue


def handle(msg):
    #State
    global node_id, node_ids,queue_pedidos,queue_respostas,waitingReads,current
    global writeLock, queue_locks, writeCurrent, currentQuorum

    if msg.body.type == 'init':
            node_id = msg.body.node_id
            node_ids = msg.body.node_ids
            waitingReads = False
            queue_pedidos = []
            queue_respostas = []
            writeLock = False
            queue_locks = []
            currentQuorum = []

            logging.info('node %s initialized', node_id)
            reply(msg,type='init_ok')

    else:
        n_quorum = math.ceil((len(node_ids)+1)/2)
        nodes = random.sample(node_ids,n_quorum)
        read_quorums = list(map(lambda x: x,nodes))
        write_quorums = list(map(lambda x: x,nodes))

        if msg.body.type == 'read':

            if msg.src not in node_ids:
                logging.info('node %s reading from %s',msg.src, msg.body.key)
                current = msg
                waitingReads = True
                for node in read_quorums:
                    send(src=node_id,dest=node,type="read",key=msg.body.key)

            else:
                logging.info('server %s reading from %s',msg.src, msg.body.key)
                if msg.body.key in dic:
                    reply(msg,type="read_ok",
                    value={
                            "value":dic[msg.body.key]["value"],
                            "timestamp":dic[msg.body.key]["timestamp"]
                        })

                else:
                    reply(msg,type="error",code=20,text="not found")
            
        elif msg.body.type == 'write':
            logging.info('node %s writing to %s',msg.src, msg.body.key)
            #logging.info(f"dicionario - {msg}")

            # @TODO Preencher currentQuorum e writeCurrent
            currentQuorum = write_quorums
            writeCurrent = msg
            for node in write_quorums:
                send(src=node_id, dest=node, type="read_lock", key = msg.body.key, value = msg.body.value)

            # if msg.body.key not in dic:
            #     dic[msg.body.key] = {"value":msg.body.value,"timestamp": 0}
            # else:
            #     ts = dic[msg.body.key]["timestamp"]
            #     dic[msg.body.key] = {"value":msg.body.value,"timestamp": ts+1}
                
            # reply(msg, type='write_ok')

        elif msg.body.type == 'cas':
            logging.info('node %s comparing and set', msg.src)

            if msg.body.key not in dic:
                reply(msg,type="error",code=20,text="not found")

            elif getattr(msg.body,"from") != dic[msg.body.key]["value"]:
                reply(msg,type="error",code=22,text="does not match")

            else:
                ts = dic[msg.body.key]["timestamp"]
                dic[msg.body.key] = {"value":msg.body.to,"timestamp": ts+1}                
                # dic[msg.body.key] = msg.body.to
                # if msg.src not in node_ids:
                #     for node in node_ids:
                #         send(src = node_id, dest = node, type="cas", key = msg.body.key, **{"from": getattr(msg.body,"from")}, to = msg.body.to)
                reply(msg,type="cas_ok")

        elif msg.body.type == 'read_lock':
            if not writeLock:
                writeLock = True
                if msg.body.key in dic:
                    reply(msg,type="lock_ok",
                    value={
                            "value":dic[msg.body.key]["value"],
                            "timestamp":dic[msg.body.key]["timestamp"]
                        })

                else:
                    reply(msg,type="noKey",text="not found")
            else:
                reply(msg,type="error",code=11,text="already locked")

        elif msg.body.type == 'lock_ok' or (msg.body.type == "error" and msg.body.code==11) or msg.body.type == "noKey":
            if msg.body.type == 'lock_ok':
                queue_locks.append((msg.body.value.value,msg.body.value.timestamp,msg.src))
            elif msg.body.type == "error":
                queue_locks.append((None,-2,msg.src))
            else:
                queue_locks.append((None,-1,msg.src))
            if len(queue_locks) == n_quorum:
                maximum = max(queue_locks,key=lambda x:x[1])
                minimum = min(queue_locks,key=lambda x:x[1])
                if minimum[1] == -2:
                    reply(writeCurrent,type="error",code=11,text="locked state")
                    for node in queue_locks:
                        send(src=node,dest=node[2],type="unlock")
                else:
                    logging.info(f"QUEUELOCK - {queue_locks}")
                    new_value = (writeCurrent.body.value,maximum[1]+1)
                    for node in currentQuorum:
                        send(src=node_id,dest=node,type="write_server",key=writeCurrent.body.key,
                        value=new_value[0],timestamp=new_value[1])
                queue_locks = []
        
        elif msg.body.type == "write_server":
            if msg.body.key not in dic or msg.body.timestamp > dic[msg.body.key]["timestamp"]:
                dic[msg.body.key] = {"value":msg.body.value,"timestamp": msg.body.timestamp}
            writeLock = False
            reply(msg,type="write_server_ok")

        elif msg.body.type == "unlock":
            writeLock = False
            reply(msg,type="unlock_ok")

        elif msg.body.type == "write_server_ok" or msg.body.type == "unlock_ok":
            queue_locks.append(msg)
            if len(queue_locks) == n_quorum:
                reply(writeCurrent,type="write_ok")
                queue_locks = []
        else:
            logging.warning('unknown message type %s', msg.body.type)

        # Caso em que está à espera das respostas dos quorums
        if waitingReads:
            if (msg.body.type == "read_ok" or msg.body.type == "error"):
                value = (msg.body.value.value,msg.body.value.timestamp) if msg.body.type != "error" else (None,-1)
                queue_respostas.append(value)
                if len(queue_respostas) == n_quorum:
                    maximum = max(queue_respostas,key=lambda x:x[1])
                    if maximum[1] == -1:
                        reply(current,type="error",code=20,text="not found")
                    else:
                        reply(current,type="read_ok",value=maximum[0])
                    waitingReads = False
                    queue_respostas = []
                    if len(queue_pedidos) > 0:
                        handle(queue_pedidos.pop(0))
            elif msg != current:
                queue_pedidos.append(msg)
                        



# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())