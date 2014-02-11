"""Brokest client library."""

import zmq
import cloud

from config.settings import CONFIG

from message import Message

def next_worker():
    """Return the next worker to send a message to."""
    workers = []
    for worker in CONFIG['WORKERS']:
        workers.append(worker)
    while True:
        for worker in workers:
            yield worker

NEXT_WORKER = next_worker()

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3


def queue(runnable, *args, **kwargs):
    """Return the result of running the task *runnable* with the given 
    arguments."""
    sequence = 0
    retries_left = REQUEST_RETRIES
    worker = next(NEXT_WORKER)
    host = worker[0]
    port = worker[1]
    message = Message(
            cloud.serialization.cloudpickle.dumps(runnable),
            args,
            kwargs)
    socket = zmq.Context().socket(zmq.REQ)
    socket.connect('tcp://{}:{}'.format(host, port))
    socket.send_pyobj(message)
    results = socket.recv_pyobj()
    return results
