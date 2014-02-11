"""Brokest client library."""
import logging

import zmq
import cloud

from config.settings import CONFIG
from message import Message

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def next_worker():
    """Return the next worker to send a message to."""
    workers = []
    for worker in CONFIG['WORKERS']:
        workers.append(worker)
    while True:
        for worker in workers:
            LOGGER.debug('Next worker is [{}]'.format(worker[2]))
            yield worker

NEXT_WORKER = next_worker()


def queue(runnable, *args, **kwargs):
    """Return the result of running the task *runnable* with the given
    arguments."""
    worker = next(NEXT_WORKER)
    host = worker[0]
    port = worker[1]

    message = Message(
            cloud.serialization.cloudpickle.dumps(runnable),
            args,
            kwargs)
    LOGGER.info('Sending [{}] with args[{}] and kwargs[{}] to {}:{}'.format(
        runnable,
        message.args,
        message.kwargs,
        host,
        port))
    socket = zmq.Context().socket(zmq.REQ)
    socket.connect('tcp://{}:{}'.format(host, port))
    socket.send_pyobj(message)
    results = socket.recv_pyobj()
    LOGGER.info('Result is [{}]'.format(results))
    return results
