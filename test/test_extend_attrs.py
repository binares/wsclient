from wsclient.extend_attrs import ExtendAttrs


class A(metaclass=ExtendAttrs):
    a = {1: 2, 11: {12: 13}}
    b = {'k': 'l'}
    c = {'x': 'y'}
    __extend_attrs__ = ['a','b']
    __deepcopy_on_init__ = ['a','b']


class B(A):
    a = {3: 4, 11: {14: 15}}
    c = {'z': 'zz'}
    __deepcopy_on_init__ = ['c']


class C(B):
    a = {31: 32}
    __extend_attrs__ = ['-a','c']

 
class D(C):
    c = {'y': 'yy'}
    __extend_attrs__ = []
    __deepcopy_on_init__ = ['-b','d']


class E(D):
    a = {51: 52}
    b = {'p': 'q'}
    c = {'x': 'xx'}
    e = {'e': {'ee': 'eee'}}
    __extend_attrs__ = ['b','-']
    __deepcopy_on_init__ = ['-','e']
    
    def __init__(self):
        self.doi =  self.__deepcopy_on_init


def test_extend_attrs():
    assert A.a == {1: 2, 11: {12: 13}}
    assert A.b == {'k': 'l'}
    assert A.c == {'x': 'y'}
    assert A._A__deepcopy_on_init == ['a','b']
    assert A.__deepcopy_on_init__ == ['a','b']
    
    assert B.a == {1: 2, 3: 4, 11: {12: 13, 14: 15}}
    assert B.b == {'k': 'l'}
    assert B.c == {'z': 'zz'}
    assert B._B__deepcopy_on_init == ['c']
    assert B.__deepcopy_on_init__ == ['a','b','c']
    
    assert C.a == {31: 32}
    assert not hasattr(C, '_C__deepcopy_on_init')
    assert C.__deepcopy_on_init__ == ['a','b','c']
    
    assert D.a == {31: 32}
    assert D.c == {'z': 'zz', 'y': 'yy'}
    assert D.__extend_attrs__ == ['b','c']
    assert D._D__deepcopy_on_init == ['-b','d']
    assert D.__deepcopy_on_init__ == ['a','c','d']
    
    assert E.a == {51: 52}
    assert E.b == {'k': 'l', 'p': 'q'}
    assert E.c == {'x': 'xx'}
    assert E._E__deepcopy_on_init == ['-','e']
    assert E.__deepcopy_on_init__ == ['e']
    
    e = E()
    assert e.doi == E._E__deepcopy_on_init
    
    # Verify that the attrs were copied
    assert e.a is E.a
    assert e.b is E.b
    assert e.c is E.c
    assert e.e == E.e
    assert e.e is not E.e
    assert e.e['e'] is not E.e['e']