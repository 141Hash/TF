#!/usr/bin/env python3

from cmath import log
from http import client
import logging
from asyncio import run, create_task, sleep
from multiprocessing.connection import wait
from wsgiref.headers import tspecials

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB(True)

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
        send(node_id, "lin-tso" ,type='ts')
        queue.append(msg)

    elif msg.body.type == 'txn_replicated':
        if msg.src != node_id:
            ctx = await db.begin([k for op,k,v in msg.body.txn], msg.src)
            await db.commit(ctx, msg.body.wv)
            db.cleanup(ctx)

        ts += 1

        if msg.src == node_id:
            reply(msg.body.client, type='txn_ok', txn = msg.body.txn)
            
        if len(waitingList) > 0:
            for element in waitingList:
                if element[1] == ts:
                    ctx = await db.begin([k for op,k,v in element[0].body.txn], element[0].src)
                    rs,wv,res = await db.execute(ctx, msg.body.txn)
                    if res:
                        await db.commit(ctx, wv)
                        for node in node_ids:
                            send(node_id, node, type = 'txn_replicated', wv = wv, txn = element[0].body.txn, client = element[0])
                    else:
                        for node in node_ids:
                            send(node_id, node, type = 'error_res')
                        reply(element[0], type='error', code=14, text='transaction aborted')

                    db.cleanup(ctx)
                    waitingList.remove(element)
                    

    elif msg.body.type == 'ts_ok':
        logging.info('TS: %s', msg.body.ts)
        req = queue.pop(0)
        waitingList.append((req, msg.body.ts))
        sorted(waitingList, key=lambda x: x[1])

        for element in waitingList:
            if element[1] == ts:
                ctx = await db.begin([k for op,k,v in element[0].body.txn], element[0].src)
                rs,wv,res = await db.execute(ctx, element[0].body.txn)
                if res:
                    await db.commit(ctx, wv)
                    for node in node_ids:
                        send(node_id, node, type = 'txn_replicated', wv = wv ,txn = element[0].body.txn, client = element[0])
                else:
                    for node in node_ids:
                        send(node_id, node, type = 'error_res')
                    reply(element[0].body.client, type='error', code=14, text='transaction aborted')

                db.cleanup(ctx)
                waitingList.remove(element)
                
    
    elif msg.body.type == 'error_res':
        ts += 1
        if len(waitingList) > 0:
            for element in waitingList: 
                if element[1] == ts:
                    ctx = await db.begin([k for op,k,v in element[0].body.txn], element[0].src)
                    rs,wv,res = await db.execute(ctx, element[0].body.txn)
                    if res:
                        await db.commit(ctx, wv)
                        for node in node_ids:
                            send(node_id, node, type = 'txn_replicated', res = res, wv = wv, txn = element[0].body.txn, client = element[0])
                    else:
                        for node in node_ids:
                            send(node_id, node, type = 'error_res')
                        reply(element[0], type='error', code=14, text='transaction aborted')
                    
                    db.cleanup(ctx)
                    waitingList.remove(element)
                    

    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
