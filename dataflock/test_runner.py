import pytest

import runner
import analysis


def test_flock_create_env():
    flock = runner.DataFlock()

    with pytest.raises(KeyError):
        flock.environment_get("test")

    e = flock.environment_create("test")
    assert e == flock.environment_get("test")

    with pytest.raises(KeyError):
        flock.environment_create("test")

    flock.environemnt_delete("test")

    with pytest.raises(KeyError):
        flock.environment_get("test")

@pytest.fixture
def env():
    flock = runner.DataFlock()
    env = flock.environment_create("test")
    env.set_dryrun()
    return env

def test_environemtn(env):
    c1 = analysis.Cell("a = 1")
    c2 = analysis.Cell("b = a + 1")

    cid = env.cell_create(c1)
    cid2 = env.cell_create(c2)

    assert cid != cid2
    assert c1 == env.cell_get(cid)

    env.cell_update(cid, c2)

    assert c1 != env.cell_get(cid)

    with pytest.raises(KeyError):
        env.cell_get('none')

    env.cell_delete(cid)

    with pytest.raises(KeyError):
        env.cell_get(cid)


def test_dependencies(env):
    c1 = analysis.Cell("a = 1")
    c2 = analysis.Cell("b = a + 1")
    c3 = analysis.Cell("c = 1")

    with pytest.raises(KeyError):
        env.exposes('a')

    cid1 = env.cell_create(c1)

    with pytest.raises(NameError):
        env.cell_create(c1)

    cid2 = env.cell_create(c2)

    assert env.exposes('a') == cid1
    assert env.exposes('b') == cid2
    assert env.depends('a') == {cid2}
    
    env.cell_update(cid2, c3)

    with pytest.raises(KeyError):
        env.exposes('b')

    assert env.exposes('c') == cid2
    assert env.depends('a') == set()
    
    
def test_walk(env):
    c1 = analysis.Cell("a = 1")
    c2 = analysis.Cell("b = a + 1")
    c3 = analysis.Cell("c = b")

    cid1 = env.cell_create(c1)
    cid2 = env.cell_create(c2)
    cid3 = env.cell_create(c3)

    assert list(env.walk(cid1)) == [cid1, cid2, cid3]

def test_walk2(env):
    c1 = analysis.Cell("a = 1")
    c2 = analysis.Cell("b = a + 1")
    c3 = analysis.Cell("c = b")
    c4 = analysis.Cell("d = b")

    cid1 = env.cell_create(c1)
    cid2 = env.cell_create(c2)
    cid3 = env.cell_create(c3)
    cid4 = env.cell_create(c4)

    print(list(env.walk(cid1)))
    print([cid1, cid2, cid3])
    assert set(env.walk(cid1)) == {cid1, cid2, cid3, cid4}

def test_loop(env):
    c1 = analysis.Cell("a = c")
    c2 = analysis.Cell("b = a + 1")
    c3 = analysis.Cell("c = b")
    c4 = analysis.Cell("d = b")
    
    cid1 = env.cell_create(c1)
    cid2 = env.cell_create(c2)

    with pytest.raises(ValueError): # loop!
        cid3 = env.cell_create(c3)
    
    cid3 = env.cell_create(c4)

    with pytest.raises(ValueError): # loop!
        cid3 = env.cell_update(cid3, c3)

def test_run(env):
    c1 = analysis.Cell("a = 1")
    c2 = analysis.Cell("b = a + 1")
    
    cid1 = env.cell_create(c1, live=False)
    cid2 = env.cell_create(c2)
    env.on_cell_run_finished(cid2)

    env.cell_run(cid1)
    assert env.is_running(cid1)
    assert not env.is_running(cid2)
    assert env.is_dirty(cid1)
    assert env.is_dirty(cid2)
    
    env.on_cell_run_finished(cid1)

    assert not env.is_running(cid1)
    assert env.is_running(cid2)
    assert not env.is_dirty(cid1)
    assert env.is_dirty(cid2)
    
    env.on_cell_run_finished(cid2)

    assert not env.is_running(cid1)
    assert not env.is_running(cid2)
    assert not env.is_dirty(cid1)
    assert not env.is_dirty(cid2)


def test_run_parallel(env):
    c1 = analysis.Cell("a = 1")
    c2 = analysis.Cell("b = a + 1")
    c3 = analysis.Cell("c = a + 1")
    c4 = analysis.Cell("d = b + c")
    
    cid1 = env.cell_create(c1, live=False)
    cid2 = env.cell_create(c2)
    env.on_cell_run_finished(cid2)
    cid3 = env.cell_create(c3)
    env.on_cell_run_finished(cid3)
    cid4 = env.cell_create(c4)
    env.on_cell_run_finished(cid4)

    env.cell_run(cid1)
    assert env.is_running(cid1)
    assert not all(env.is_running(c) for c in [cid2, cid3, cid4])
    assert all(env.is_dirty(c) for c in [cid1, cid2, cid3, cid4])
    
    env.on_cell_run_finished(cid1)

    assert all(env.is_running(c) for c in [cid2, cid3])
    assert not all(env.is_running(c) for c in [cid1, cid4])
    assert all(env.is_dirty(c) for c in [cid2, cid3, cid4])
    assert not all(env.is_dirty(c) for c in [cid1])
    
    env.on_cell_run_finished(cid2)

    assert all(env.is_running(c) for c in [cid3])
    assert not all(env.is_running(c) for c in [cid1, cid2, cid4])
    assert all(env.is_dirty(c) for c in [cid3, cid4])
    assert not all(env.is_dirty(c) for c in [cid1, cid2])
    
    env.on_cell_run_finished(cid3)

    assert all(env.is_running(c) for c in [cid4])
    assert not all(env.is_running(c) for c in [cid1, cid2, cid3])
    assert all(env.is_dirty(c) for c in [cid4])
    assert not all(env.is_dirty(c) for c in [cid1, cid2, cid3])
    
    env.on_cell_run_finished(cid4)

    assert not all(env.is_running(c) for c in [cid1, cid2, cid3, cid4])
    assert not all(env.is_dirty(c) for c in [cid1, cid2, cid3, cid4])
    

def test_live(env):
    c1 = analysis.Cell("a = 1")
    c2 = analysis.Cell("b = a + 1")
    c3 = analysis.Cell("c = b + 1")
    
    cid1 = env.cell_create(c1)
    cid2 = env.cell_create(c2, live=False)
    cid3 = env.cell_create(c3)

    assert env.is_running(cid1)
    print("aa")
    env.on_cell_run_finished(cid1)
    print("bb")
    assert not env.is_running(cid1)
    assert not env.is_running(cid2)
    print("bb")
    env.cell_run(cid2)

    assert env.is_running(cid2)
    
    env.on_cell_run_finished(cid2)

    assert env.is_running(cid3)
    
