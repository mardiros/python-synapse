#!/usr/bin/env python
import logging
import gevent.coros
from gevent_zeromq import zmq

from synapse.node import Node, NodeException, registerNode as regNode

_context = zmq.Context()


class ZMQNode(Node):
    """Node built on top of ZeroMQ.

    ZeroMQ provides a socket API to build several kinds of topology.

    """
    type = 'zmq'

    def __init__(self, config):
        Node.__init__(self, config)
        self._socket = None
        self._lock = gevent.coros.Semaphore()
        self._log = logging.getLogger(self.name)

    @property
    def socket(self):
        return self._socket

    def send(self, msg):
        """
        Send a message
        :param  dst: object that contains a send() socket interface
        :param  msg: serializable string

        """
        self._lock.acquire()
        ret = self._socket.send(msg)
        self._lock.release()
        return ret

    def recv(self):
        """
        Return a message as a string from the receiving queue.

        Blocks on the underlying ``self._socket.recv()``, that's why it waits
        on a event that will be woke up by the poller.

        """
        self._log.debug('waiting in recv()')
        self._lock.acquire()
        msgstring = self._socket.recv()
        self._log.debug('socket: %s' % self._socket)
        self._log.debug('recv -> %s' % msgstring)
        self._lock.release()
        return msgstring

    def __repr__(self):
        return "<%s %s (%s)>" % (self.__class__.__name__,
                                 self._name,
                                 self._uri)


class ZMQServer(ZMQNode):

    def __init__(self, config, handler):
        ZMQNode.__init__(self, config)
        self._handler = handler

    def start(self):
        self._socket = _context.socket(zmq.REP)
        self._socket.bind(self._uri)

    def loop(self):
        while True:
            try:
                self._log.debug('in server loop')
                raw_request = self.recv()
                raw_reply = self._handler(raw_request)
            except NodeException, err:
                errmsg = str(err.errmsg)
                self._log.debug(errmsg)
                raw_reply = err.reply

            if raw_reply:
                self._socket.send(raw_reply)

    def send(self, msg):
        raise NotImplementedError()

    def stop(self):
        self._socket.close()


class ZMQClient(ZMQNode):

    def connect(self):
        self._socket = _context.socket(zmq.REQ)
        self._socket.connect(self._uri)
        # client side do not need to be registred, they are not looping
        #poller.register(self)

    def close(self):
        self._socket.close()


class ZMQPublish(ZMQNode):
    """Prove the publish side of a PUB/SUB topology.

    Behave as a server. Support only :meth:`send`. :meth:`start` do not take
    any handler as the publisher produces messages.

    """
    def start(self):
        self._socket = _context.socket(zmq.PUB)
        self._socket.bind(self._uri)

    def recv(self):
        raise NotImplementedError()

    def stop(self):
        self._socket.close()


class ZMQSubscribe(ZMQNode):
    """Provide the subscribe side of a PUB/SUB topology.

    Behave as a client. It connects to the remote publish side and listens to
    incoming messages in the queue. :meth:`connect` takes the handler that will
    be called when a message arrives. Support only :meth:`recv`.

    """
    def __init__(self, config, handler):
        ZMQNode.__init__(self, config)
        self._handler = handler

    def connect(self):
        self._socket = _context.socket(zmq.SUB)
        self._socket.connect(self._uri)
        self._socket.setsockopt(zmq.SUBSCRIBE, '')
        print self._uri

    def loop(self):
        while True:
            self._log.debug('in subscriber loop')
            raw_request = self.recv()
            self._handler(raw_request)

    def send(self, msg):
        raise NotImplementedError

    def close(self):
        self._socket.close()

def registerNode():
    regNode('zmq', {'roles': {'client':   ZMQClient,
                              'server':   ZMQServer,
                              'publish':  ZMQPublish,
                              'subscribe': ZMQSubscribe
                              }})
