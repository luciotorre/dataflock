import multiprocessing
import uuid
import zmq
import json

OUT_PORT = "8989"
IN_PORT = "8990"

class Environment:
    def __init__(self):
        self.context = zmq.Context()
        self.send_address = "tcp://127.0.0.1:%s" % IN_PORT
        self.out_socket = self.context.socket(zmq.PUB)
        self.out_socket.bind(self.send_address)
        
        self.recv_address = "tcp://127.0.0.1:%s" % OUT_PORT
        self.in_socket = self.context.socket(zmq.SUB)
        self.in_socket.setsockopt_string(zmq.SUBSCRIBE, "env")
        self.in_socket.bind(self.recv_address)

    def close(self):
        print("CLOSED")
        self.in_socket.close()
        self.out_socket.close()

    def create_context(self):
        return Context(self, self)

    def new_kernel(self):
        kid = str(uuid.uuid4())
        k = Kernel(kid, self, {})
        multiprocessing.Process(target=k.run).start()
        print("waiting")
        self.in_socket.recv_string() # waiting for it to be ready
        print("kernel ready")
        return kid

    def publish(self, kid, message):
        self.socket.send_string(kid + " " + json.dumps(message))

    def message(self, kid, cmd, *args, **kwargs):
        msg = kid + " " + json.dumps((cmd, args, kwargs))
        print("sending:", msg)
        self.out_socket.send_string(msg)
        data = self.in_socket.recv_string()
        r_args, r_kwargs = json.loads(data[4:])
        print("GOT", r_args, r_kwargs)
        return r_args, r_kwargs


class Kernel:
    def __init__(self, kid, environment, local_vars):
        self.kid = kid
        self.env = environment
        self.local_vars = local_vars
        
    def reply(self, *args, **kwargs):
        print("sending", args)
        self.out_socket.send_string("env " + json.dumps((args, kwargs)))

    def run(self):
        self.context = zmq.Context()
        
        self.out_socket = self.context.socket(zmq.PUB)
        self.out_socket.connect(self.env.recv_address)

        self.in_socket = self.context.socket(zmq.SUB)
        self.in_socket.connect(self.env.send_address)        
        topicfilter = self.kid
        self.in_socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
        import time
        time.sleep(1)
        self.reply("ready")
        
        while True:
            data = self.in_socket.recv_string()
            print("INMESSAGE:", data)
            json_data = data[data.index(" ") + 1:]
            cmd, args, kwargs = json.loads(json_data)

            if cmd == 'fork':  
                k = Kernel(args[0], self.env, self.local_vars)
                multiprocessing.Process(target=k.run).start()
                print("STARTERD")
            elif cmd == 'kill':
                self.reply("ok")
                return
            elif cmd == 'run':
                exec(args[0], globals(), self.local_vars)
                self.reply("ok")
            elif cmd == 'get':
                print("GOTing")
                value = self.local_vars[args[0]]
                self.reply(value)
                print("..")


class Context:
    def __init__(self, environment, parent):
        self.env = environment
        self.parent = parent
        self.children = []
        self.kid = parent.new_kernel()     

    def create_context(self):
        return Context(self.env, self)

    def new_kernel(self):
        kid = str(uuid.uuid4())
        self.env.message(self.kid, 'fork', kid)
        return kid
  
    def run(self, code):
        self.env.message(self.kid, "run", code)

    def get(self, varname):
        return self.env.message(self.kid, "get", varname)[0][0]

    def kill(self):
        self.env.message(self.kid, "kill")
        

class Cell:
    def __init__(self, code, depends, exposes):
        self.code = code
        self.depends = depends
        self.exposes = exposes


