from fons.func import get_arg_count
import asyncio


class URLFactory:
    def __init__(self, wrapper, url_notation, params=None):
        """:type wrapper: WSClient
           :type url_notation: str"""
        self.wc = wrapper
        self.notation = url_notation
        self.params = params
        
        self.parts = self.split(url_notation)
        decoded = [self.decode_part(x) for x in self.parts]
        self.types = [x[0] for x in decoded]
        self.names = [x[1] for x in decoded]
        
        
    async def create_url(self, params):
        """
        :returns: a dict with `url` and `extra_headers`
        """
        if params is None:
            params = self.params
        
        str_parts = []
        headers = {}
        
        for name,type in zip(self.names,self.types):
            # Each resolved can be either
            #  - <str> (appended to the url)
            #  - <dict> (extended to headers)
            #  - None (ignored)
            # or a list of those
            if type == 'plain':
                resolved = name
            elif type == 'attribute':
                resolved = getattr(self.wc, name)
            elif type in ('variable',):
                resolved = params[name]
            elif type == 'component':
                cmp = self.wc.url_components[name]
                if hasattr(cmp,'__call__'):
                    args = [self] if get_arg_count(cmp) else []
                    cmp = (await cmp(*args)) if asyncio.iscoroutinefunction(cmp) else cmp(*args)
                resolved = cmp
            elif type in ('method', 'method:shared'):
                method = getattr(self.wc, name)
                args = [self] if get_arg_count(method) else []
                resolved = (await method(*args)) if asyncio.iscoroutinefunction(method) else method(*args)
            else:
                raise ValueError(type)
            
            if isinstance(resolved, (str, dict)) or resolved is None:
                resolved = [resolved]
            
            for x in resolved:
                if x is None:
                    continue
                elif isinstance(x, dict):
                    headers.update(x)
                else:
                    str_parts.append(x)
        
        url = ''.join(str_parts)
        
        config = {
            'url': url,
        }
        if headers:
            config['extra_headers'] = headers
        
        return config
    
    @staticmethod
    def split(url):
        parts = []
        while url:
            i = url.find('<')
            j = url[i:].find('>')
            if i == -1 or j == -1:
                parts.append(url)
                break
            j += i
            preceding = url[:i]
            if preceding:
                parts.append(preceding)
            var = url[i:j+1]
            if j-i > 1:
                parts.append(var)
            url = url[j+1:]
        return parts
    
    @staticmethod
    def decode_part(x):
        if not x.startswith('<'):
            return 'plain', x
        
        x = x[1:-1]
        
        if x[:1] == '$' or x[:2] == 'c$':
            return 'component', x[x.find('$')+1:]
        
        elif x[:2] == 'a$':
            return 'attribute', x[2:]
        
        elif x[:2] != 'm$':
            return 'variable', x
        
        shared = ':shared' if x.endswith(':shared') else ''
        if shared:
            x = x[:-len(':shared')]
        
        if x[:2] == 'm$':
            return 'method{}'.format(shared), x[2:]
        #else:
        #    return 'variable{}'.format(shared), x
        
        
    def copy(self, params=None):
        if params is None:
            params = self.params
        return self.__class__(self.wc, self.notation, params)
    
    
    def __eq__(self, other):
        if not isinstance(other, URLFactory):
            raise TypeError(type(other))
        
        if self.names != other.names or self.types != other.types:
            return False
        
        if None not in (self.params, other.params) and self.params==other.params:
            return True
        
        for obj in (self,other):
            for name,type in zip(obj.names, obj.types):
                if type=='method':
                    return False
                elif type=='variable':
                    v1 = self.params.get(name)
                    v2 = other.params.get(name)
                    if None in (v1, v2) or v1 != v2:
                        return False
                elif type == 'component' and not isinstance(self.wc.url_components[name], str):
                    return False
                
        return True
    
    
    def __call__(self):
        #Note that it returns coroutine
        return self.create_url(self.params)
