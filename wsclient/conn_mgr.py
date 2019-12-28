from fons.dict_ops import deep_get
from fons.event import Station

from .conn import Connection

# How to integrate both url and socket sub registration?
# When unsubbing remove it from cnxi's url_factory, which
# will then later .satisfies(sub), but return e.g. 'through_socket'
# which will signal to .send() to proceed with the through-socket
# procedure

class ConnectionManager:
    def __init__(self, wrapper):
        """:type wrapper: WSClient"""
        self.ww = wrapper
        self.connections = {}
        self.cnx_infs = {}
        self.station = Station([{'channel': 'all_active', 'id': 0, 'queue': False}],
                               loops=[self.ww.loop],
                               name=self.ww.name+'[CM][Station]')
    
    def add_connection(self, config, start=False):
        cnx = Connection(**config)
        self.connections[cnx.id] = cnx
        self.cnx_infs[cnx] = ConnectionInfo(self.ww, cnx)
        if start:
            cnx.start()
        return cnx
    
    def remove_connection(self, id, delete=False):
        cnx = None
        if not isinstance(id, Connection):
            try: cnx = self.connections[id]
            except KeyError: pass
        else:
            cnx = id
        if cnx is not None:
            cnx.stop()
            if delete and cnx.id in self.connections:
                del self.connections[cnx.id]
            if delete and cnx in self.cnx_infs:
                del self.cnx_infs[cnx]
        return cnx
    
    def remove_all(self):
        for id in list(self.connections.keys()):
            self.remove_connection(id)
            
    def delete_connection(self, cnx):
        try: del self.connections[cnx.id]
        except KeyError: pass
        
        try: del self.cnx_infs[cnx]
        except KeyError: pass
        
        if cnx.is_running():
            cnx.stop()
            
            
class ConnectionInfo:
    def __init__(self, ww, cnx):
        """:type ww: WSClient
           :type cnx: Connection"""
        self.ww = ww
        self.cnx = cnx
        self.max_subscriptions = (self.ww.sh.max_subs_per_socket 
                                    if self.ww.sh.max_subs_per_socket is not None else 
                                  100*1000)
        self.authenticated = False
        
    def satisfies(self, params, url_factory=None):
        """:param url_factory: may provide params specific url_factory
                               which shall yield True if cnx.url.params == url_factory.params
                               (useful for reusing old connection for the exact same subscription,
                                if the url directly provides stream (takes only one sub, enabled by connecting))"""
        channel = params['_']
        #send = self.ww.cis.get_value(channel,'send',True)
        #register_via = self.ww.cis.get_value(channel,'register_via','socket').lower()
        #methods = register_via.split(' ')
        #url_satisfied = True
        #if 'url' in methods:
        if url_factory is None:
            url_factory = self.ww.cis.get_value(channel,'url_factory')
        if self.cnx.url != url_factory:
            return False
        auth_satisfied = self.auth_satisfied(channel)
        if not auth_satisfied:
            return False
        return True
        
    def auth_satisfied(self, channel):
        is_private = self.ww.cis.get_value(channel,'is_private')
        seq = self.auth_seq(channel)
        auth_required = deep_get(seq,'required',return2=None)
        if (not is_private and auth_required is None) or \
                (auth_required is not None and not auth_required):
            return True
        via_url = deep_get(seq,'via_url')
        if via_url: 
            return True
        each_time = deep_get(seq,'each_time')
        if each_time or self.authenticated:
            return True
        #One for authentication subscription, the other for current subscription
        elif self.free_subscription_slots >= 2:
            return True
        return False        
    
    def is_private(self):
        pass
    
    def auth_seq(self, channel, i=None):
        auth = self.ww.cis.get_value(channel,'auth',{})
        defaults = self.ww.auth_defaults
        if i is None:
            return [auth, defaults]
        else:
            auth_i = auth.get(i, {})
            defaults_i = defaults.get(i, {})
            return [auth_i, auth, defaults_i, defaults]
    
    def get_auth_value(self, channel, *args, **kw):
        """Deep get auth value of channel"""
        return deep_get(self.auth_seq(channel),*args,**kw)
        
    @property
    def connection(self):
        return self.cnx
    @property
    def sub_count(self):
        return sum(s.cnx is self.cnx for s in self.ww.sh.subscriptions)
    @property
    def free_subscription_slots(self):
        return self.max_subscriptions - self.sub_count
