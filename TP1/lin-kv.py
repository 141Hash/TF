#!/usr/bin/env python3

# Simple 'echo' workload in Python for Maelstrom

from asyncore import write
from cgitb import text
from cmath import log
from distutils.log import info
import imp
from inspect import currentframe
import logging
from concurrent.futures import ThreadPoolExecutor
from operator import itemgetter
from types import SimpleNamespace
from numpy import maximum

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
    global node_id, node_ids, queue_respostas, current, currentReadQuorum
    global lock, queue_locks_write, writeCurrent, currentWriteQuorum
    global currentCasQuorum, casCurrent, queue_locks_cas

    if msg.body.type == "init":
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        queue_respostas = []
        lock = False
        queue_locks_write = []
        currentWriteQuorum = []
        currentReadQuorum = []
        queue_locks_cas = []

        logging.info("node %s initialized", node_id)
        reply(msg, type="init_ok")

    else:
        n_quorum = math.ceil((len(node_ids) + 1) / 2)
        nodes = random.sample(node_ids, n_quorum)
        read_quorums = list(map(lambda x: x, nodes))
        write_quorums = list(map(lambda x: x, nodes))
        cas_quorums = list(map(lambda x: x, nodes))

        if msg.body.type == "read":
            logging.info("node %s reading from %s", msg.src, msg.body.key)
            current = msg
            currentReadQuorum = read_quorums
            for node in read_quorums:
                send(src=node_id, dest=node, type="read_server", key=msg.body.key)

        elif msg.body.type == "read_server":
            logging.info("server %s reading from %s", msg.src, msg.body.key)
            if not lock:
                lock = True
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
            else:
                reply(msg, type="error_read_lock", text="already_locked")

        elif (
            msg.body.type == "read_server_ok"
            or msg.body.type == "error_read"
            or msg.body.type == "error_read_lock"
        ):
            if msg.body.type == "read_server_ok":
                queue_respostas.append(
                    (msg.body.value.value, msg.body.value.timestamp, msg.src)
                )
            elif msg.body.type == "error_read":
                queue_respostas.append((None, -1, msg.src))
            else:
                queue_respostas.append((None, -2, msg.src))

            if len(queue_respostas) == n_quorum:
                maximum = max(queue_respostas, key=lambda x: x[1])
                minimum = min(queue_respostas, key=lambda x: x[1])
                if minimum[1] == -2:
                    for node in queue_respostas:
                        if node[1] != -2:
                            send(src=node_id, dest=node[2], type="unlock")
                    reply(current, type="error", code=11, text="locked state")

                elif maximum[1] == -1:
                    for node in queue_respostas:
                        send(src=node_id, dest=node[2], type="unlock_read", code=20)
                else:
                    for node in queue_respostas:
                        send(
                            src=node_id,
                            dest=node[2],
                            type="unlock_read",
                            code=0,
                            value=maximum[0],
                        )
                queue_respostas = []

        elif msg.body.type == "unlock_read":
            lock = False
            if msg.body.code == 0:
                reply(msg, type="unlock_read_ok", code=0, value=msg.body.value)
            else:
                reply(msg, type="unlock_read_ok", code=msg.body.code)

        elif msg.body.type == "unlock_read_ok":
            queue_respostas.append(msg)
            if len(queue_respostas) == n_quorum:
                if msg.body.code == 0:
                    reply(current, type="read_ok", value=msg.body.value)
                elif msg.body.code == 20:
                    reply(current, type="error", code=20, text="not found")
                queue_respostas = []

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
            if not lock:
                lock = True
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
                reply(msg, type="error_write_lock", text="already locked")

        elif (
            msg.body.type == "lock_ok"
            or msg.body.type == "error_write_lock"
            or msg.body.type == "noKey"
        ):
            if msg.body.type == "lock_ok":
                queue_locks_write.append(
                    (msg.body.value.value, msg.body.value.timestamp, msg.src)
                )
            elif msg.body.type == "error_write_lock":
                queue_locks_write.append((None, -2, msg.src))
            else:
                queue_locks_write.append((None, -1, msg.src))
            if len(queue_locks_write) == n_quorum:
                maximum = max(queue_locks_write, key=lambda x: x[1])
                minimum = min(queue_locks_write, key=lambda x: x[1])
                if minimum[1] == -2:
                    for node in queue_locks_write:
                        if node[1] != -2:
                            send(src=node_id, dest=node[2], type="unlock")
                    reply(writeCurrent, type="error", code=11, text="locked_state")
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
            lock = False
            reply(msg, type="write_server_ok")

        elif msg.body.type == "write_server_ok":
            queue_locks_write.append(msg)
            if len(queue_locks_write) == n_quorum:
                reply(writeCurrent, type="write_ok")
                queue_locks_write = []

        elif msg.body.type == "cas":
            logging.info("node %s comparing and setting", msg.src)

            currentCasQuorum = cas_quorums
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
            if not lock:
                lock = True
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
                reply(msg, type="error_cas_lock", text="already locked")

        elif (
            msg.body.type == "lock_ok_cas"
            or msg.body.type == "noMatch"
            or msg.body.type == "noKeyCas"
            or msg.body.type == "error_cas_lock"
        ):
            if msg.body.type == "lock_ok_cas":
                queue_locks_cas.append(
                    (msg.body.value.value, msg.body.value.timestamp, msg.src)
                )
            elif msg.body.type == "error_cas_lock":
                queue_locks_cas.append((None, -3, msg.src))
            elif msg.body.type == "noKeyCas":
                queue_locks_cas.append((None, -1, msg.src))
            elif msg.body.type == "noMatch":
                queue_locks_cas.append((None, -2, msg.src))

            if len(queue_locks_cas) == n_quorum:
                maximum = max(queue_locks_cas, key=lambda x: x[1])
                minimum = min(queue_locks_cas, key=lambda x: x[1])
                if minimum[1] == -3:
                    for node in queue_locks_cas:
                        if node[1] != -2:
                            send(src=node_id, dest=node[2], type="unlock")
                    reply(casCurrent, type="error", code=11, text="locked state")
                elif maximum[1] == -2:
                    for node in queue_locks_cas:
                        send(src=node_id, dest=node[2], type="unlock_cas", code=22)
                elif maximum[1] == -1:
                    for node in queue_locks_cas:
                        send(src=node_id, dest=node[2], type="unlock_cas", code=20)
                else:
                    for node in currentCasQuorum:
                        send(
                            src=node_id,
                            dest=node,
                            type="cas_server",
                            key=casCurrent.body.key,
                            **{"from": getattr(casCurrent.body, "from")},
                            to=casCurrent.body.to,
                            timestamp=maximum[1] + 1
                        )
                queue_locks_cas = []

        elif msg.body.type == "unlock_cas":
            lock = False
            reply(msg, type="unlock_cas_ok", code=msg.body.code)

        elif msg.body.type == "cas_server":
            dic[msg.body.key] = {"value": msg.body.to, "timestamp": msg.body.timestamp}
            lock = False
            reply(msg, type="cas_server_ok")

        elif msg.body.type == "cas_server_ok" or msg.body.type == "unlock_cas_ok":
            queue_locks_cas.append(msg)
            if len(queue_locks_cas) == n_quorum:
                if msg.body.type == "cas_server_ok":
                    reply(casCurrent, type="cas_ok")
                else:
                    if msg.body.code == 20:
                        reply(casCurrent, type="error", code=20, text="not found")
                    elif msg.body.code == 22:
                        reply(casCurrent, type="error", code=22, text="does not match")
                queue_locks_cas = []

        elif msg.body.type == "unlock":
            lock = False


# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())
