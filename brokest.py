"""Broker-less distributed task queue."""
import pickle

import zmq
import cloud

HOST = '127.0.0.1'
PORT = 9090
TASK_SOCKET = zmq.Context().socket(zmq.REQ)
TASK_SOCKET.connect('tcp://{}:{}'.format(HOST, PORT))

class Worker(object):
    """A remote task executor."""

    def __init__(self, host=HOST, port=PORT):
        """Initialize worker."""
        self.host = host
        self.port = port
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)

    def start(self):
        """Start listening for tasks."""
        self._socket.bind('tcp://{}:{}'.format(self.host, self.port))
        while True:
            runnable_string = self._socket.recv_pyobj()
            runnable = pickle.loads(runnable_string)
            self._socket.send_pyobj('')
            args = self._socket.recv_pyobj()
            self._socket.send_pyobj('')
            kwargs = self._socket.recv_pyobj()
            response = self._do_work(runnable, args, kwargs)
            self._socket.send_pyobj(response)

    def _do_work(self, task, args, kwargs):
        """Return the result of executing the given task."""
        print('Running [{}] with args [{}] and kwargs [{}]'.format(
            task, args, kwargs))
        return task(*args, **kwargs)

def queue(runnable, *args, **kwargs):
    """Return the result of running the task *runnable* with the given 
    arguments."""
    runnable_string = cloud.serialization.cloudpickle.dumps(runnable)
    TASK_SOCKET.send_pyobj(runnable_string)
    TASK_SOCKET.recv()
    TASK_SOCKET.send_pyobj(args)
    TASK_SOCKET.recv()
    TASK_SOCKET.send_pyobj(kwargs)
    results = TASK_SOCKET.recv_pyobj()
    return results

if __name__ == '__main__':
    w = Worker()
    w.start()
