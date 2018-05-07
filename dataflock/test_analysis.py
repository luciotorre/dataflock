from textwrap import dedent

from analysis import Cell, find_missing_vars


def test_exposed_variables():
    text = dedent("""
        hola = 1
        chau = hola + mundo + cruel
    """)

    c = Cell(text)
    assert c.exposes == {"chau", "hola"}


def test_depend_variables_are_found_using_analysis(mocker):
    fake_find_missing = mocker.patch('analysis.find_missing_vars', autospec=True)

    text = dedent("""
        hola = 1
        chau = hola + mundo + cruel
    """)

    c = Cell(text)
    assert c.depends == fake_find_missing.return_value


def test_builtins_are_ignored():
    sample_code = """
sum([10, 20])
"""
    assert find_missing_vars(sample_code) == set()


def test_simple_assigned_and_unassigned_vars():
    sample_code = """
a = 10
b = 5
c = a + d  # operation with one undefined var
print(a)
print(b)
print(c)
print(e)  # undefined var as param
"""
    assert find_missing_vars(sample_code) == set(['d', 'e'])


def test_deletion_counts_as_usage():
    sample_code = """
a = 10
del a
del b  # undefined var deletion
"""
    assert find_missing_vars(sample_code) == set(['b'])


def test_unassigned_var_when_similar_assigned_var_but_in_different_scope():
    sample_code = """
def sample():
    a = 10
    b = 20
print(a)  # this var isn't the same from the function
class Thing():
    c = 10
    def another_sample():
        print(b)  # var defined in another scope
print(c)  # this var isn't the same from the class
"""
    assert find_missing_vars(sample_code) == set(['a', 'b', 'c'])


def test_tuple_assignments():
    sample_code = """
a, *b, c = 10, 11, 12, 13
print(a)
print(b)
print(c)
"""
    assert find_missing_vars(sample_code) == set()


def test_assigned_vars_are_missing_after_deletion():
    sample_code = """
a = 10
b = 11
del b
print(a)
print(b)  # deleted var as param
"""
    assert find_missing_vars(sample_code) == set(['b'])


def test_deletion_only_removes_from_corresponding_scope():
    sample_code = """
a = 10
def sample():
    a = 10
    del a
print(a)
b = 10
class Thing():
    b = 10
    del b
print(b)
"""
    assert find_missing_vars(sample_code) == set()


def test_if_isnt_a_different_scope():
    sample_code = """
a = 10
if a:  # defined var in header
    print(a)  # defined var inside
    b = 10  # var defined inside
print(b)  # var defined inside used outside

if c:  # undefined var in header
    print(d)  # undefined var inside
"""
    assert find_missing_vars(sample_code) == set(['c', 'd'])


def test_if_block_is_optional_so_dels_could_be_not_executed():
    sample_code = """
a = 10
if True:
    del a
print(a)
"""
    assert find_missing_vars(sample_code) == set()


def test_for_loop_isnt_a_different_scope():
    sample_code = """
a = [1, 2, 3]
for x in a:  # defined var in header
    print(a)  # defined var inside
    b = 10  # var defined inside
print(b)  # var defined inside used outside

for x in c:  # undefined var in header
    print(d)  # undefined var inside
"""
    assert find_missing_vars(sample_code) == set(['c', 'd'])


def test_for_loop_block_is_optional_so_dels_could_be_not_executed():
    sample_code = """
a = [1, 2, 3]
for x in a:
    del a
print(a)
"""
    assert find_missing_vars(sample_code) == set()


def test_for_loop_header_vars_leak():
    sample_code = """
sample = [1, 2, 3]
for a, b in sample:
    pass
print(a)
print(b)
"""
    assert find_missing_vars(sample_code) == set()


def test_while_isnt_a_different_scope():
    sample_code = """
a = 10
while a:  # defined var in header
    print(a)  # defined var inside
    b = 10  # var defined inside
print(b)  # var defined inside used outside

while c:  # undefined var in header
    print(d)  # undefined var inside
"""
    assert find_missing_vars(sample_code) == set(['c', 'd'])


def test_while_block_is_optional_so_dels_could_be_not_executed():
    sample_code = """
a = 10
while True:
    del a
print(a)
"""
    assert find_missing_vars(sample_code) == set()


def test_context_manager_isnt_a_different_scope():
    sample_code = """
a = 10
with a() as b:  # defined var in header
    print(a)  # defined var inside
    print(b)  # header defined var inside
    c = 10  # var defined inside
print(c)  # var defined inside used outside

while d:  # undefined var in header
    print(e)  # undefined var inside
"""
    assert find_missing_vars(sample_code) == set(['d', 'e'])


def test_context_manager_block_isnt_optional_so_dels_will_always_be_executed():
    sample_code = """
a = 10
with a() as b:
    del a
    del b
print(a)
print(b)
"""
    assert find_missing_vars(sample_code) == set(['a', 'b'])


def test_context_manager_header_vars_leak():
    sample_code = """
a = 10
with a() as b:
    pass
print(b)
"""
    assert find_missing_vars(sample_code) == set()


def test_try_isnt_a_different_scope():
    sample_code = """
a = 10
try:
    print(a)  # defined var inside
    b = 10  # var defined inside
except:
    pass
print(b)  # var defined inside used outside

try:
    print(c)  # undefined var inside
except:
    pass
"""
    assert find_missing_vars(sample_code) == set(['c'])


def test_try_block_is_optional_so_dels_could_be_not_executed():
    sample_code = """
a = 10
try:
    b = 1/0
    del a
except:
    pass
print(a)
"""
    assert find_missing_vars(sample_code) == set()


def test_except_isnt_a_different_scope():
    sample_code = """
a = 10
try:
    pass
except:
    print(a)  # defined var inside
    b = 10  # var defined inside
print(b)  # var defined inside used outside

try:
    pass
except:
    print(c)  # undefined var inside
"""
    assert find_missing_vars(sample_code) == set(['c'])


def test_except_block_is_optional_so_dels_could_be_not_executed():
    sample_code = """
a = 10
try:
    pass
except:
    del a
print(a)
"""
    assert find_missing_vars(sample_code) == set()


def test_finally_isnt_a_different_scope():
    sample_code = """
a = 10
try:
    pass
except:
    pass
finally:
    print(a)  # defined var inside
    b = 10  # var defined inside
print(b)  # var defined inside used outside

try:
    pass
except:
    pass
finally:
    print(c)  # undefined var inside
"""
    assert find_missing_vars(sample_code) == set(['c'])


def test_finally_block_isnt_optional_so_dels_will_always_be_executed():
    sample_code = """
a = 10
try:
    pass
except:
    pass
finally:
    del a
print(a)
"""
    assert find_missing_vars(sample_code) == set(['a'])


def test_try_except_finally_share_scope_downwards_between_them():
    sample_code = """
try:
    a = 10
except:
    print(a)
    b = 10
finally:
    print(a)
    print(b)
"""
    assert find_missing_vars(sample_code) == set()


def test_function_parameters_are_set_variables():
    sample_code = """
def sample(a):
    print(a)
"""
    assert find_missing_vars(sample_code) == set()


def test_classes_and_functions_are_detected_as_variabes_set():
    sample_code = """
def sample():
    def sub_sample():
        sub_sample()
        sample()
sample()
class Sample():
    pass
Sample()
"""
    assert find_missing_vars(sample_code) == set()


def test_attributes_arent_analyzed_as_undefined_variables():
    sample_code = """
class Sample():
    pass
a = Sample()
print(a.name)
"""
    assert find_missing_vars(sample_code) == set()


def test_complex_nested_scopes_handled_right():
    sample_code = """
a = 10
def sample(b):
    c = 10
    class Sample():
        print(a)
        print(b)
        print(c)
"""
    assert find_missing_vars(sample_code) == set()


# TODO globals vs locals
#   - set a global from local scope?
#   - del a global from local scope?
