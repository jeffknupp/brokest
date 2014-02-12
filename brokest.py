"""Brokest client library."""
import logging

import zmq
import cloud

from message import Message

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
HOST = '127.0.0.1'
PORT = 7070

def queue(runnable, *args, **kwargs):
    """Return the result of running the task *runnable* with the given
    arguments."""
    message = Message(
            cloud.serialization.cloudpickle.dumps(runnable),
            args,
            kwargs)
    LOGGER.info('Sending [{}] with args[{}] and kwargs[{}] to {}:{}'.format(
        runnable,
        message.args,
        message.kwargs,
        HOST,
        PORT))
    socket = zmq.Context().socket(zmq.REQ)
    socket.connect('tcp://{}:{}'.format(HOST, PORT))
    socket.send_pyobj(message)
    results = socket.recv_pyobj()
    LOGGER.info('Result is [{}]'.format(results))
    return results

if __name__ == '__main__':
    context = zmq.Context()

    t = zmq.devices.ProcessMonitoredQueue(zmq.ROUTER, zmq.DEALER, zmq.PUB)
    t.bind_in('tcp://127.0.0.1:7070')
    t.bind_out('tcp://127.0.0.1:7080')
    t.bind_mon('tcp://127.0.0.1:9090')
    t.start()

    # We never get here
    frontend.close()
    backend.close()
    context.term()

