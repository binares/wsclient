from fons.dict_ops import deep_update
from fons.py import mro as _mro

import copy as _copy

PROPERTY_RECURSION_LIMIT = 2

_ASC = ''.join(chr(i) for i in range(128))
_ALNUM_ = ''.join(x for x in _ASC if x.isalnum()) + '_'
_ALPHA_ = ''.join(x for x in _ASC if x.isalpha()) + '_'
_ALNUM_DOT_ = _ALNUM_ + '.'



class ExtendAttrs(type):
    """
    Deep updates attributes listed in __extend_attrs__, by copying the next attribute found in mro,
    and then "deep updates" it with the same attr of current class. 
    IMPORTANT!
    All these attributes are being deepcopied, i.e. all references to previous attrs 
    and their (deep) values are lost.
    """
    def __new__(cls, name, bases, attrs):
        mro = _mro(*bases)
        mro_included = (attrs,) + mro
        extend_attrs = ExtendAttrs._join(mro_included, '__extend_attrs__')
        
        for _ in extend_attrs:
            try: nxt_value = next(getattr(x, _) for x in mro if hasattr(x, _))
            except StopIteration: continue
            if _ in attrs:
                attrs[_] = deep_update(nxt_value, attrs[_], copy=True)
            else:
                attrs[_] = _copy.deepcopy(nxt_value)
        
        if '__extend_attrs__' in attrs:
            # Save the original value
            attrs['_{}__extend_attrs'.format(name)] = attrs['__extend_attrs__']
            # Overwrite with new value
            attrs['__extend_attrs__'] = extend_attrs
        
        if '__deepcopy_on_init__' in attrs:
            attrs['_{}__deepcopy_on_init'.format(name)] = attrs['__deepcopy_on_init__']
            deepcopy_on_init = ExtendAttrs._join(mro_included, '__deepcopy_on_init__')
            attrs['__deepcopy_on_init__'] = deepcopy_on_init
       
        return super(ExtendAttrs, cls).__new__(cls, name, bases, attrs)
    
    
    @staticmethod
    def _join(bases, name='__extend_attrs__'):
        """Adds all previous __extend_attrs__ together, and deducts all that start with '-'"""
        extend_attrs = []
        
        for cls in reversed(bases):
            is_dict = isinstance(cls, dict)
            _name = name if is_dict else '_{}{}'.format(cls.__name__, name[:-2])
            cls_ea = cls.get(_name) if is_dict else getattr(cls, _name, None)
            
            if cls_ea is None:
                continue
            
            if '-' in cls_ea:
                extend_attrs = []
            
            deduct = [x for x in cls_ea if len(x)>1 and x[0]=='-']
            add = [x for x in cls_ea if not x.startswith('-')]
            
            for attr in deduct:
                try: extend_attrs.remove(attr[1:])
                except ValueError: pass
            
            for attr in add:
                if attr not in extend_attrs:
                    extend_attrs.append(attr)
                
        return extend_attrs


class CreateProperties(type):
    """Initiates properties listed in __properties__. Also creates sub_to_{x} and unsub_to{x} shortcuts to all
    methods that match the patterns subscribe_to_{x} / unsubscribe_to_{x}"""
    def __new__(cls, name, bases, attrs):
        CreateProperties._init_properties(attrs)
       
        return super(CreateProperties, cls).__new__(cls, name, bases, attrs)
    
    
    @staticmethod
    def _init_properties(attrs):
        properties = attrs.get('__properties__', [])
        final_properties = properties[:]
        
        #Create properties for methods that start with "(un)subscribe_to"
        for attr in attrs:
            for startsWith,replaceWith in zip(['subscribe_to_','unsubscribe_to_'],
                                              ['sub_to_','unsub_to_']):
                if attr.startswith(startsWith):
                    property_name = replaceWith + attr[len(startsWith):]
                    final_properties.append([property_name,attr,True,False,False])
                    
        for item in final_properties:
            property_name = item[0]
            attrs[property_name] = CreateProperties._create_property(*item)
    
    
    @staticmethod            
    def _verify_name(name, set=_ALNUM_):
        if not all(x in set for x in name):
            raise ValueError("The property name/value '{}' contains non alnum characters".format(name))
                             
        if not all(x[:1] in _ALPHA_ for x in name.split('.')):
            raise ValueError("The property name/value '{}' starts with non alpha character".format(name))
    
    
    @staticmethod       
    def _create_property(property_name, attr, GET=True, SET=True, DEL=True):
        """
        This MAY be vulnerable to attack, if the x.y.z... leads to another
        property (that executes code), or to who knows what object.
        Make sure that the __properties__ are trusted 
        (which they as pre-assigned class attributes almost certainly should be,
         unless we are talking about hypothetical dynamic subclassing through an API,
         or a malevolent github team member in a situation where other members
         didn't turn attention to __properties__ modification).
        For that reason the PROPERTY_RECURSION_LIMIT is set to 2."""
        CreateProperties._verify_name(property_name)
        #attr can be given as attr_of_self.attr_of_that_attr....
        CreateProperties._verify_name(attr, _ALNUM_DOT_)
        
        attr_seq = attr.split('.')
        if len(attr_seq) > PROPERTY_RECURSION_LIMIT:
            raise ValueError('Property "{}" value "{}" > PROPERTY_RECURSION_LIMIT ({})' \
                             .format(property_name, attr, PROPERTY_RECURSION_LIMIT))
        last_attr = attr_seq[-1]
        
        def _getattr(self, attr_seq=attr_seq):
            obj = self
            for x in attr_seq:
                obj = getattr(obj, x)
            return obj
        
        def _setattr(self, value):
            pre_last_obj = _getattr(self, attr_seq[:-1])
            setattr(pre_last_obj, last_attr, value)
            
        def _delattr(self):
            pre_last_obj = _getattr(self, attr_seq[:-1])
            delattr(pre_last_obj, last_attr)
            
        args = [None, None, None]
        if GET:
            args[0] = _getattr
        if SET:
            args[1] = _setattr
        if DEL:
            args[2] = _delattr
            
        return property(*args)
    
    
class WSMeta(CreateProperties, ExtendAttrs):
    pass