import pytest 
from contextlib import contextmanager
import inspect 

import subrpc


def test_command():
    cmd = subrpc.Command.new_command("cmd", 'arg', kw='arg')
    
    assert cmd.cmd == 'cmd'
    assert cmd.args[0] == 'arg'
    assert cmd.kwargs['kw'] == 'arg'


@contextmanager
def rpc(slave_class):
    rpc = subrpc.get_master_for(slave_class)
    try:
        yield rpc
    finally:
        rpc.kill()


class RpcTestSlave(subrpc.SubRPCSlave):
    async def do_echo(self, what):
        return what

    async def do_raise(self):
        1 / 0

@pytest.mark.asyncio
async def test_methods():
    with rpc(RpcTestSlave) as echo:
        assert inspect.ismethod(echo.do_echo)
    

@pytest.mark.asyncio
async def test_execution():
    with rpc(RpcTestSlave) as echo:
        result = await echo.do_echo("hello")
    
    assert result == "hello"

@pytest.mark.asyncio
async def test_execution():
    with rpc(RpcTestSlave) as echo:
        with pytest.raises(subrpc.RemoteException):
            result = await echo.do_raise()
    
@pytest.mark.asyncio
async def test_execution():
    with rpc(RpcTestSlave) as echo:
        result = await echo.do_raise()