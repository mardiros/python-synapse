import random
import simplejson as json



class Message(object):
    pass



class HelloMessage(Message):
    type = 'hello'
    def __init__(self, src, uri):
        self.src = src
        self.uri = uri


    @property
    def msgtype(self):
        return self.type


    @property
    def msgattrs(self):
        return {
            'src': self.src,
            'uri': self.uri}


    def loads(self, msg):
        pass


    def dumps(self, msg):
        pass



class ByeMessage(Message):
    type = 'bye'
    def __init__(self, src):
        pass



class WhereIsMessage(Message):
    type = 'where_is'
    def __init__(self, src, params):
        pass



class MessageCodec(object):
    def loads(self, msgstring):
        raise NotImplementedError()


    def dumps(self, msg):
        raise NotImplementedError()



class MessageCodecJSONRPC(MessageCodec):
    def __init__(self, config):
        pass


    @property
    def ids(self):
        rand_min = 0
        rand_max = 2**16

        while True:
            n = random.randint(rand_min, rand_max)
            yield n


    def loads(self, msgstring):
        jsonrpc_msg = json.loads(msgstring)
        msgtype = jsonrpc_msg['method']
        msgattrs = jsonrpc_msg['params']
        msg_dict = {'type': msgtype}
        msg_dict.update(msgattrs)
        msg = makeMessage(msg_dict)
        return msg


    def dumps(self, msg):
        jsonrpc_msg = {
            'method': msg.msgtype,
            'params': msg.msgattrs,
            'id': self.ids.next()}

        return json.dumps(jsonrpc_msg)



def makeCodec(config):
    dispatch = {
        'jsonrpc': MessageCodecJSONRPC,
    }
    return dispatch[config['type']](config)



def makeMessage(msg):
    msgtmp = msg.copy()
    subclasses = Message.__subclasses__()
    dispatch = dict((cls.type, cls)for cls in subclasses)
    msgtype = msgtmp['type']
    del msgtmp['type']
    return dispatch[msgtype](**msgtmp)
