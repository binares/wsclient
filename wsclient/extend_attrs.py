from fons.dict_ops import deep_update
from fons.py import mro as _mro

import copy as _copy


class ExtendAttrs(type):
    """Deep updates attributes listed in __extend_attrs__, by copying the next attribute found in mro,
     and then "deep updates" it with the same attr of current class. 
     IMPORTANT: All these attributes are being deepcopied, i.e. all references to previous attrs 
     and their (deep) values are lost."""
    def __new__(cls, name, bases, attrs):
        mro = _mro(*bases)
        if '__extend_attrs__' in attrs:
            to_extend = attrs['__extend_attrs__']
        else:
            to_extend = next((x.__extend_attrs__ for x in mro if hasattr(x,'__extend_attrs__')),[])
        
        for _ in to_extend:
            try: nxt_value = next(getattr(x,_) for x in mro if hasattr(x,_))
            except StopIteration: continue
            if _ in attrs:
                attrs[_] = deep_update(nxt_value,attrs[_],copy=True)
            else:
                attrs[_] = _copy.deepcopy(nxt_value)
                            
        return super(ExtendAttrs, cls).__new__(cls, name, bases, attrs)
