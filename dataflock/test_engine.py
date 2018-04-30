from textwrap import dedent
import psutil

import pytest
import engine

import resource
import humanize


@pytest.fixture
def env():
    e = engine.Environment()
    yield e
    e.close()

def test_one_context(env):
    print("creating")
    ctx = env.create_context()
    print("running")
    ctx.run("a = 1")
    print("getting")
    assert ctx.get("a") == 1
    print("done")
    ctx.kill()


def test_two_context(env):
    ctx = env.create_context()
    ctx.run("a = 1")
    ctx2 = ctx.create_context()
    ctx2.run("b = a + 1")
    assert ctx2.get("b") == 2
    print("alldone")
    ctx.kill()
    ctx2.kill()

def test_three_context(env):
    ctx = env.create_context()
    ctx.run("a = [0]")
    ctx2 = ctx.create_context()
    ctx2.run("a.append(2); b = len(a)")
    ctx3 = ctx.create_context()
    ctx3.run("b = len(a)")
    assert ctx2.get("b") == 2
    assert ctx3.get("b") == 1
    print("alldone")
    ctx.kill()
    ctx2.kill()
    ctx3.kill()

def dump(parent):
    for p in parent.children():
        print(
            p.name(), 
            humanize.naturalsize(p.memory_info().rss),
            humanize.naturalsize(p.memory_info().shared),
        )
        dump(p)
    
def hmem(what):
    total = 0
    #print("WHAT", what)
    print(psutil.virtual_memory())
    #dump(psutil.Process())
        
def test_mem(env):
    ctx = env.create_context()

    hmem("PRE")
    ctx.run(dedent("""
    import numpy as np

    arr = np.ones((1000,1000,500))
    result = np.sum(arr)
    """))

    hmem("POST1")
    input()
    ctx2 = ctx.create_context()
    ctx2.run(dedent("""
    arr[::] = 0
    result = np.sum(arr)
    """))
    
    hmem("POST2")
    input()
    ctx3 = ctx.create_context()
    ctx3.run(dedent("""
    result = np.sum(arr)
    """))

    hmem("POST3")
    input()
    assert ctx2.get("result") == 0
    assert ctx3.get("result") == ctx.get("result")
    print("alldone")
    input()
    ctx.kill()
    ctx2.kill()
    ctx3.kill()
