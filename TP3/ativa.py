#!/usr/bin/env python3

from http import client
import logging
from asyncio import run, create_task, sleep
from multiprocessing.connection import wait
from wsgiref.headers import tspecials

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB()

async def handle(msg):
    # State
    global node_id, node_ids
    global db
    global ts, queue, waitingList

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        ts = 0
        queue = []
        waitingList = []
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    
    elif msg.body.type == 'txn':
        if msg.src not in node_ids:
            send(node_id, "lin-tso" ,type='ts')
            queue.append(msg)

        else:
            ctx = await db.begin([k for op,k,v in msg.body.txn], msg.src+'-'+str(msg.body.mensagem))
            rs,wv,res = await db.execute(ctx, msg.body.txn)
            if res:
                await db.commit(ctx, wv)
                reply(msg, type='txn_replicated_ok', txn=res, client = msg.body.client)
            else:
                for node in node_ids:
                    send(node_id, node, type = 'error_res')
                reply(msg.body.client, type='error', code=14, text='transaction aborted')

            ts += 1
            
            if len(waitingList) > 0:
                for element in waitingList:
                    if element[1] == ts:
                        for node in node_ids:
                            send(node_id, node, type = 'txn', txn = element[0].body.txn, mensagem = element[0].body.msg_id, client = element[0])

                        waitingList.remove(element)
                        break

            db.cleanup(ctx)

    elif msg.body.type == 'txn_replicated_ok':
        reply(msg.body.client, type='txn_ok', txn = msg.body.txn)

    elif msg.body.type == 'ts_ok':
        logging.info('TS: %s', msg.body.ts)
        req = queue.pop(0)
        waitingList.append((req, msg.body.ts))

        for element in waitingList:
            if element[1] == ts:
                for node in node_ids:
                    send(node_id, node, type = 'txn', txn = element[0].body.txn, mensagem = element[0].body.msg_id, client = element[0])

                waitingList.remove(element)
                break
    
    elif msg.body.type == 'error_res':
        ts += 1
        if len(waitingList) > 0:
            for element in waitingList:
                if element[1] == ts:
                    for node in node_ids:
                        send(node_id, node, type = 'txn', txn = element[0].body.txn, mensagem = element[0].body.msg_id, client = element[0])

                    waitingList.remove(element)
                    break

    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
