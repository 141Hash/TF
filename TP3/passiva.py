#!/usr/bin/env python3

from http import client
import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB()

async def handle(msg):
    # State
    global node_id, node_ids
    global db

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    
    elif msg.body.type == 'txn':
        if msg.src not in node_ids:
            for node in node_ids:
                send(node_id, node, type = 'txn', txn = msg.body.txn, mensagem = msg.body.msg_id, client = msg)
        else:
            send(node_id, "lin-tso" ,type='ts')
            ctx = await db.begin([k for op,k,v in msg.body.txn], msg.src+'-'+str(msg.body.mensagem))
            rs,wv,res = await db.execute(ctx, msg.body.txn)
            if res:
                await db.commit(ctx, wv)
                reply(msg, type='txn_replicated_ok', txn=res, client = msg.body.client)
            else:
                reply(msg, type='error', code=14, text='transaction aborted')
            db.cleanup(ctx)

    elif msg.body.type == 'txn_replicated_ok':
        reply(msg.body.client, type='txn_ok', txn = msg.body.txn)

    elif msg.body.type == 'ts_ok':
        logging.info('TS: %s', msg.body.ts)
        
    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
