#!/usr/bin/env python

from unittest import TestCase


class ZMQTestCase(TestCase):

    def test_registerNode(self):
        from synapse.zmq_node  import registerNode
        from synapse import node
        registerNode()
        self.assertTrue('zmq' in node.node_registry)
        del node.node_registry['zmq']

    def test_zmqnode(self):
        from synapse.zmq_node import ZMQNode

        conf = {'uri': 'ipc://./test.unix',
                'name': 'zmqnode'}

        class DummySocket(object):

            def send(self, msg):
                return "dummy_socket_send"

            def recv(self):
                return "dummy_socket_recv"

        n = ZMQNode(conf)
        self.assertEquals(n.name, conf['name'])
        self.assertEquals(n.uri, conf['uri'])
        self.assertEquals(repr(n), "<ZMQNode zmqnode (ipc://./test.unix)>")

        dummysocket = DummySocket()
        n._socket = dummysocket
        self.assertEquals(n.socket, dummysocket)
        self.assertEquals(n.send(""), "dummy_socket_send")
        self.assertEquals(n.recv(), "dummy_socket_recv")

    def test_zmqserver_sync(self):
        import gevent
        from synapse.zmq_node import ZMQServer, ZMQClient
        conf_srv = {'uri': 'tcp://*:5555',
                    'name': 'zmq_srv_sync'}

        conf_cli = {'uri': 'tcp://localhost:5555',
                    'name': 'zmq_cli_sync'}

        def srv_handler(msg):
            return "sync_response"

        server = ZMQServer(conf_srv, srv_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError, server.send,
                          "unimplemented")

        serverlet = gevent.spawn(server.loop)

        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("message")

        response = client.recv()
        self.assertEquals(response, "sync_response")
        gevent.kill(serverlet)
        client.close()
        server.stop()

    def test_zmqserver_async(self):
        import gevent
        from synapse.node import async
        from synapse.zmq_node import ZMQServer, ZMQClient
        conf_srv = {'uri': 'tcp://*:5556',
                    'name': 'zmq_srv_async'}

        conf_cli = {'uri': 'tcp://localhost:5556',
                    'name': 'zmq_cli_async'}

        @async
        def srv_handler(msg):
            return "async_response"

        server = ZMQServer(conf_srv, srv_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError, server.send,
                          "unimplemented")

        serverlet = gevent.spawn(server.loop)

        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("message")
        response = client.recv()
        self.assertEquals(response, "async_response")
        gevent.sleep(1)
        gevent.kill(serverlet)
        client.close()
        server.stop()

    def test_zmqserver_exc(self):
        import gevent
        from synapse.node import NodeException
        from synapse.zmq_node import ZMQServer, ZMQClient
        conf_srv = {'uri': 'tcp://*:5557',
                    'name': 'zmqsrv'}

        conf_cli = {'uri': 'tcp://localhost:5557',
                    'name': 'zmqcli'}

        def srv_handler(msg):
            raise NodeException("buggy", "exc_result")

        server = ZMQServer(conf_srv, srv_handler)
        self.assertTrue(server.socket is None)
        server.start()
        self.assertTrue(server.socket is not None)
        self.assertRaises(NotImplementedError, server.send,
                          "unimplemented")

        serverlet = gevent.spawn(server.loop)

        client = ZMQClient(conf_cli)
        self.assertTrue(client.socket is None)
        client.connect()
        self.assertTrue(client.socket is not None)
        client.send("sync_message")
        response = client.recv()
        self.assertEquals(response, "exc_result")
        gevent.kill(serverlet)
        client.close()
        server.stop()

    def test_zmq_publish(self):
        import gevent
        from synapse.zmq_node import ZMQPublish, ZMQSubscribe
        conf_pub = {'uri': 'tcp://127.0.0.1:5560',
                    'name': 'zmq_pub'}

        conf_sub = {'uri': 'tcp://127.0.0.1:5560',
                    'name': 'zmq_sub'}

        class Hdl:
            def __init__(self):
                self.msg = None

            def sub_handler(self, msg):
                hdl.msg = msg

        hdl = Hdl()

        pub = ZMQPublish(conf_pub)
        self.assertTrue(pub.socket is None)
        pub.start()
        pub.send("puslished_message")

        self.assertTrue(pub.socket is not None)
        self.assertRaises(NotImplementedError, pub.recv)

        sub = ZMQSubscribe(conf_sub, hdl.sub_handler)
        self.assertTrue(sub.socket is None)
        sub.connect()
        self.assertTrue(sub.socket is not None)

        self.assertRaises(NotImplementedError, sub.send, "")

        subglet = gevent.spawn(sub.loop)
        gevent.sleep(1)
        pub.send("puslished_message")
        gevent.sleep(1)
        self.assertEquals(hdl.msg, "puslished_message")
        subglet.kill()

        pub.stop()
        sub.close()


class FactoryTestCase(TestCase):

    def test_makeNode(self):
        from synapse.node import makeNode
