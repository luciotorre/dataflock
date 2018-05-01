from textwrap import dedent

import analysis


def test_variables():
    text = dedent("""
        hola = 1
        chau = hola + mundo + cruel
    """) 

    c = analysis.Cell(text)
    
    assert c.depends == {"mundo", "cruel"}
    assert c.exposes == {"chau", "hola"}