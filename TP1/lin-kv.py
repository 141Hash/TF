#!/usr/bin/env python3

# Simple 'echo' workload in Python for Maelstrom

from asyncore import write
from cmath import log
from distutils.log import info
import imp
from inspect import currentframe
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
executor = ThreadPoolExecutor(max_workers=1)

dic = dict()

# Mandar pedido para todos os quorums
# Esperar pela resposta de todos
# Se ainda estiver à espera mandar pedidos para queue
# Quando receber todas as respostas, pegar ultimo pedido da queue


def handle(msg):
    # State
    global node_id, node_ids, queue_pedidos, queue_respostas, waitingReads, current, currentReadQuorum
    global writeLock, queue_locks_write, writeCurrent, currentWriteQuorum
    global currentCasQuorum, casCurrent, queue_locks_cas

    if msg.body.type == "init":
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        waitingReads = False
        queue_pedidos = []
        queue_respostas = []
        writeLock = False
        queue_locks_write = []
        currentWriteQuorum = []
        currentReadQuorum = []

        logging.info("node %s initialized", node_id)
        reply(msg, type="init_ok")

    else:
        n_quorum = math.ceil((len(node_ids) + 1) / 2)
        nodes = random.sample(node_ids, n_quorum)
        read_quorums = list(map(lambda x: x, nodes))
        write_quorums = list(map(lambda x: x, nodes))

        if msg.body.type == "read":

            logging.info("node %s reading from %s", msg.src, msg.body.key)
            current = msg
            waitingReads = True
            currentReadQuorum = read_quorums
            for node in read_quorums:
                send(src=node_id, dest=node, type="read_server", key=msg.body.key)

        elif msg.body.type == "read_server":
            logging.info("server %s reading from %s", msg.src, msg.body.key)
            if msg.body.key in dic:
                reply(
                    msg,
                    type="read_server_ok",
                    value={
                        "value": dic[msg.body.key]["value"],
                        "timestamp": dic[msg.body.key]["timestamp"],
                    },
                )

            else:
                reply(msg, type="error_read", code=20, text="not found")

        elif (
            msg.body.type == "read_server_ok" or msg.body.type == "error_read"
        ) and msg.src in currentReadQuorum:
            value = (
                (msg.body.value.value, msg.body.value.timestamp)
                if msg.body.type != "error_read"
                else (None, -1)
            )
            queue_respostas.append(value)
            if len(queue_respostas) == n_quorum:
                maximum = max(queue_respostas, key=lambda x: x[1])
                if maximum[1] == -1:
                    reply(current, type="error", code=20, text="not found")
                else:
                    reply(current, type="read_ok", value=maximum[0])
                waitingReads = False
                queue_respostas = []
                if len(queue_pedidos) > 0:
                    handle(queue_pedidos.pop(0))

        elif msg.body.type == "write":
            logging.info("node %s writing to %s", msg.src, msg.body.key)

            currentWriteQuorum = write_quorums
            writeCurrent = msg
            for node in write_quorums:
                send(
                    src=node_id,
                    dest=node,
                    type="read_lock",
                    key=msg.body.key,
                    value=msg.body.value,
                )

        elif msg.body.type == "read_lock":
            if not writeLock:
                writeLock = True
                if msg.body.key in dic:
                    reply(
                        msg,
                        type="lock_ok",
                        value={
                            "value": dic[msg.body.key]["value"],
                            "timestamp": dic[msg.body.key]["timestamp"],
                        },
                    )

                else:
                    reply(msg, type="noKey", text="not found")
            else:
                reply(msg, type="error", code=11, text="already locked")

        elif (
            msg.body.type == "lock_ok"
            or (msg.body.type == "error" and msg.body.code == 11)
            or msg.body.type == "noKey"
        ):
            if msg.body.type == "lock_ok":
                queue_locks_write.append(
                    (msg.body.value.value, msg.body.value.timestamp, msg.src)
                )
            elif msg.body.type == "error":
                queue_locks_write.append((None, -2, msg.src))
            else:
                queue_locks_write.append((None, -1, msg.src))
            if len(queue_locks_write) == n_quorum:
                maximum = max(queue_locks_write, key=lambda x: x[1])
                minimum = min(queue_locks_write, key=lambda x: x[1])
                if minimum[1] == -2:
                    reply(writeCurrent, type="error", code=11, text="locked state")
                    for node in queue_locks_write:
                        send(src=node, dest=node[2], type="unlock")
                else:
                    new_value = (writeCurrent.body.value, maximum[1] + 1)
                    for node in currentWriteQuorum:
                        send(
                            src=node_id,
                            dest=node,
                            type="write_server",
                            key=writeCurrent.body.key,
                            value=new_value[0],
                            timestamp=new_value[1],
                        )
                queue_locks_write = []

        elif msg.body.type == "write_server":
            if (
                msg.body.key not in dic
                or msg.body.timestamp > dic[msg.body.key]["timestamp"]
            ):
                dic[msg.body.key] = {
                    "value": msg.body.value,
                    "timestamp": msg.body.timestamp,
                }
            writeLock = False
            reply(msg, type="write_server_ok")

        elif msg.body.type == "unlock":
            writeLock = False
            reply(msg, type="unlock_ok")

        elif msg.body.type == "write_server_ok" or msg.body.type == "unlock_ok":
            queue_locks_write.append(msg)
            if len(queue_locks_write) == n_quorum:
                reply(writeCurrent, type="write_ok")
                queue_locks_write = []

        elif msg.body.type == "cas":
            logging.info("node %s comparing and setting", msg.src)

            currentCasQuorum = write_quorums + read_quorums
            casCurrent = msg
            for node in currentCasQuorum:
                send(
                    src=node_id,
                    dest=node,
                    type="cas_lock",
                    key=msg.body.key,
                    **{"from": getattr(msg.body, "from")},
                    to=msg.body.to
                )

        elif msg.body.type == "cas_lock":
            if not writeLock:
                writeLock = True
                if msg.body.key not in dic:
                    reply(msg, type="noKeyCas")

                elif getattr(msg.body, "from") != dic[msg.body.key]["value"]:
                    reply(msg, type="noMatch")

                else:
                    reply(
                        msg,
                        type="lock_ok_cas",
                        value={
                            "value": dic[msg.body.key]["value"],
                            "timestamp": dic[msg.body.key]["timestamp"],
                        },
                    )

            else:
                reply(msg, type="error", code=11, text="already locked")

        else:
            queue_pedidos.append(msg)


# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())
