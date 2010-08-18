"""
Provides Node and Actor.

All objects share the same global _context and _loop. Non-blocking and
asynchronous events handler are registered in the global _loop. The event loop
allows to handle multiple requests in a single requests.

"""
import zmq

from message import makeMessage, makeCodec, \
                    HelloMessage, ByeMessage, WhereIsMessage



class EventLoop(object):
    def __init__(self, run_in_thread=False):
        from zmq.eventloop import ioloop
        self._loop = ioloop.IOLoop.instance()
        self._is_started = False
        self._thread = None
        if run_in_thread:
            import threading
            self._thread = threading.Thread(target=self._loop.start)


    def add_recv_handler(self, socket, handler):
        self._loop.add_handler(socket, handler, zmq.POLLIN)


    def start(self):
        if not self._is_started:
            if self._thread:
                self._thread.start()
            else:
                self._loop.start()
            self._is_started = True



_context = zmq.Context()
_loop = EventLoop()



class Node(object):
    """Node abstract interface.
    A node is part of a network e.g. a graph that connects objects. It can
    receive messages from or send messages to other nodes connected by an edge.
    :meth:`setup` is called to configure the specific underlying protocol.

    """
    def setup(self):
        raise NotImplementedError()


    def send(self, dst, msg):
        raise NotImplementedError()


    def recv(self, src):
        raise NotImplementedError()



class Actor(object):
    """
    An actor receives messages in its mailbox.

    In response to a message it receives, an actor can make local decisions,
    create more actors, send more messages, and determine how to respond to the
    next message received.

    :IVariables:
    - `name`: the name that identifies the current node
    - `uri`: defines the protocol address of the current node
    - `mailbox`: object that provide access to messages
    - `nodes`: list of other nodes
    - `announce`: subscribed queue of announces where new nodes introduce
      themselves

    """
    def __init__(self, config):
        self._uri = config['uri']
        self._mailbox = makeNode({
                'type': config['type'],
                'uri':  self._uri,
                'role': 'server'
                })
        self._announce = AnnounceClient(config['announce'])
        self.nodes = {}



class AnnounceServer(object):
    """
    The announce server listens to messages and publish them to all connected
    nodes.

    :IVariables:
    - `server`
    - `publisher`
    - `codec`
    - `nodes`

    """
    def __init__(self, config):
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._server = makeNode({
                'type': config['type'],
                'uri':  config['uri'],
                'role': 'server'
                })
        self._publisher = makeNode({
                'type': config['type'],
                'uri':  config['announce']['uri'],
                'role': 'publish'
                })
        self._nodes = {}


    def setup(self):
        self._server.setup()
        self._publisher.setup()
        return self


    def start(self):
        self._publisher.start()
        self._server.start(self.handle_message)


    def handle_message(self, socket, events):
        msgstring = socket.recv()
        msg = self._codec.loads(msgstring)
        print msg.type
        socket.send('ack')



class AnnounceClient(object):
    """
    The announce service localizes the nodes in the network. When a node joins
    the network it sends a 'hello' message. The 'hello' message is published to
    all other nodes through the announce queue.

    When a node wants to know the URI of another node it sends a 'where_is'
    message.

    :IVariables:
    - `subscriber`
    - `client`
    - `codec`

    """
    def __init__(self, config):
        self._codec = makeCodec({
                'type': config['codec']
                })
        self._subscriber = makeNode({
                'type': config['type'],
                'uri':  config['announce']['uri'],
                'role': 'subscribe',
                })
        self._client = makeNode({
                'type': config['type'],
                'uri':  config['uri'],
                'role': 'client'
                })


    def connect(self):
        self._subscriber.setup()
        self._subscriber.connect(self.handle_announce)
        self._client.setup().connect()


    def send_to(self, dst, msg):
        return dst.send(self._codec.dumps(msg))


    def recv_from(self, src):
        return self._codec.loads(src.recv())


    def hello(self, node):
        msg = HelloMessage(node.name, node.uri)
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def bye(self, node):
        msg = ByeMessage(node.name)
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def where_is(node_name):
        msg = WhereIsMessage(node.name, params={'who': other_node_name})
        self.send_to(self._client, msg)
        return self.recv_from(self._client)


    def handle_announce(self, socket, events):
        print socket.recv()



class ZMQNode(Node):
    """Node built on top of ZeroMQ.

    ZeroMQ provides a socket API to build several kinds of topology.

    """
    type = 'zmq'

    def __init__(self, config):
        self._uri = config['uri']
        self._socket = None


    @property
    def uri(self):
        return self._uri


    @property
    def socket(self):
        return self._socket


    def send(self, msg):
        """Send a message
        @param  dst: object that contains a send() socket interface
        @param  msg: serializable string

        """
        self._socket.send(msg)


    def recv(self):
        """Receive a message
        @param  src: object that contains a recv() socket interface
        @rtype: str

        """
        return self._socket.recv()



def mixIn(target, mixin_class):
    if mixin_class not in target.__bases__:
        target.__bases__ = (mixin_class,) + target.__bases__
    return target



def makeNode(config):
    dispatch = {'zmq': {
                'class': ZMQNode,
                'roles': {
                    'client':   ZMQClient,
                    'server':   ZMQServer,
                    'publish':  ZMQPublish,
                    'subscribe':ZMQSubscribe}}}

    cls = dispatch[config['type']]['class']
    if 'role' in config:
        cls = dispatch[config['type']]['roles'][config['role']]

    return cls(config)



class ZMQServer(ZMQNode):
    def setup(self):
        self._socket = _context.socket(zmq.REP)
        self._socket.bind(self._uri)
        return self


    def start(self, handler):
        if not self._socket:
            self.setup()
        _loop.add_recv_handler(self._socket, handler)



class ZMQClient(ZMQNode):
    def setup(self):
        self._socket = _context.socket(zmq.REQ)
        return self

    def connect(self):
        if not self._socket:
            self.setup()
        self._socket.connect(self._uri)


    def __repr__(self):
        return '<%s: %s>' % (type(self), self.uri)



class ZMQPublish(ZMQNode):
    """Prove the publish side of a PUB/SUB topology.

    Behave as a server. Support only :meth:`send`. :meth:`start` do not take
    any handler as the publisher produces messages.

    """
    def setup(self):
        self._socket = _context.socket(zmq.PUB)
        return self


    def start(self):
        if not self._socket:
            self.setup()


    def recv(self):
        raise NotImplementedError()



class ZMQSubscribe(ZMQNode):
    """Provide the subscribe side of a PUB/SUB topology.

    Behave as a client. It connects to the remote publish side and listens to
    incoming messages in the queue. :meth:`connect` takes the handler that will
    be called when a message arrives. Support only :meth:`recv`.

    """
    def setup(self):
        self._socket = _context.socket(zmq.SUB)
        self._socket.bind(self._uri)
        return self


    def connect(self, handler):
        if not self._socket:
            self.setup()
        self._socket.connect(self._uri)
        self._socket.setsockopt(zmq.SUBSCRIBE, '')
        _loop.add_recv_handler(self._socket, handler)


    def send(self, msg):
        raise NotImplementedError()
