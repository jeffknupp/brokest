"""Broker-less distributed task queue."""
from __future__ import absolute_import
import pickle
import logging
import multiprocessing

import zmq
import cloud
from config.settings import CONFIG


LOGGER = logging.getLogger(__name__)

class Worker(object):
    """A remote task executor."""

    def __init__(self, host, port, worker_id=0):
        """Initialize worker."""
        print 'starting worker [{}]'.format(worker_id)
        LOGGER.info('Starting worker [{}]'.format(worker_id))
        self.host = host
        self.port = port
        self._id = worker_id
        self._context = None
        self._socket = None

    def start(self):
        """Start listening for tasks."""
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
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
        print('[{}] running [{}] with args [{}] and kwargs [{}]'.format(self._id,
            task, args, kwargs))
        return task(*args, **kwargs)

def next_worker():
    workers = []
    for worker in CONFIG['WORKERS']:
        workers.append(worker)
    while True:
        for worker in workers:
            yield worker

NEXT_WORKER = next_worker()

def queue(runnable, *args, **kwargs):
    """Return the result of running the task *runnable* with the given 
    arguments."""
    worker = next(NEXT_WORKER)
    host = worker[0]
    port = worker[1]
    socket = zmq.Context().socket(zmq.REQ)
    print host, port
    socket.connect('tcp://{}:{}'.format(host, port))
    runnable_string = cloud.serialization.cloudpickle.dumps(runnable)
    socket.send_pyobj(runnable_string)
    socket.recv()
    socket.send_pyobj(args)
    socket.recv()
    socket.send_pyobj(kwargs)
    results = socket.recv_pyobj()
    return results

if __name__ == '__main__':
    workers = []
    for worker in CONFIG['WORKERS']:
        w = Worker(worker[0], worker[1], worker[2])
        process = multiprocessing.Process(target=w.start)
        process.start()
     
