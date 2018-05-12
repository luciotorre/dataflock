import multiprocessing
import json
from collections import namedtuple
import asyncio
import aioprocessing
import uuid
import inspect
import functools
import types
import traceback


class Command(namedtuple('Command', ['id', 'cmd', 'args', 'kwargs'])):

    @classmethod
    def new_command(cls, cmd, *args, **kwargs):
        uid = uuid.uuid4().hex
        self = cls(uid, cmd, args, kwargs)
        return self

class Response(namedtuple('Response', ['cmd', 'result'])):
    pass


class RemoteExceptionData(namedtuple('RemoteException', ['cmd', 'exception'])):
    pass

class RemoteException(Exception):
    def __init__(self, data):
        self.args = data['args']
        self.repr = data['repr']
        
    def __str__(self):
        return self.repr


class SubRPCSlave:
    def __init__(self, channel, stdout, stderr):
        self.channel = channel
        self.stdout = stdout
        self.stderr = stderr

    async def call(self, cmd):
        try:
            func = getattr(self, cmd.cmd)
            try:
                result = await func(*cmd.args, **cmd.kwargs)
            except Exception as e:
                print(dir(e))
                evt = dict(
                    args=e.args,
                    repr=traceback.format_exc()
                )
                self.channel.send(RemoteExceptionData(cmd, evt))
            else:
                self.channel.send(Response(cmd, result))
        except Exception as e:
            print("wtf", e)

    async def _start(self):
        while True:
            cmd = await self.channel.coro_recv()
            asyncio.ensure_future(self.call(cmd))

    def start(self):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._start())
        

class SubRPCMaster:
    def __init__(self, slave):
        self.slave = slave

        for name, method in inspect.getmembers(slave): #, predicate=inspect.ismethod):
            if name.startswith("do_"):
                def get_func(_name, _method):
                    @functools.wraps(_method)
                    async def proxy(slf, *args, **kwargs):
                        return await slf.cmd(_name, *args, **kwargs)
                    return proxy
                    
                proxy = types.MethodType(get_func(name, method), self)
                setattr(self, name, proxy)

        self.start()
        
    async def listen_task(self):
        while True:
            response = await self.channel.coro_recv()
            cid = response.cmd.id
            future = self.pending_cmds.pop(cid)
            if isinstance(response, Response):
                future.set_result(response.result)
            else:
                exc = RemoteException(response.exception)
                future.set_exception(exc)

    def interrupt(self):
        pass

    def restart(self):
        self.kill()
        self.start()

    def start(self):
        self.pending_cmds = {}
        self.channel, client_channel = aioprocessing.AioPipe()
        self.stdout_q = sout = aioprocessing.AioQueue()
        self.stderr_q = serr = aioprocessing.AioQueue()
        self.listener = asyncio.ensure_future(self.listen_task())
        loop = asyncio.get_event_loop()
        kernel = self.slave(client_channel, sout, serr)
        self.process = p = aioprocessing.AioProcess(target=kernel.start)
        p.start()

    def kill(self):
        self.channel = None
        self.stdout_q = None
        self.stderr_q = None
        self.process.terminate()
        self.listener.cancel()

    async def cmd(self, cmd_name, *args, **kwargs):
        cmd = Command.new_command(cmd_name, *args, **kwargs)
        response = asyncio.Future()
        self.pending_cmds[cmd.id] = response
        self.channel.send(cmd)
        return await response


def get_master_for(slave):
    return SubRPCMaster(slave)

