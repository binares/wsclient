from fons.dict_ops import deep_get


class ChannelsInfo:
    def __init__(self, wrapper):
        """:type wrapper: WSClient"""
        self.wc = wrapper
        
    def create_id_tuple(self, params):
        id_kwargs = self._fetch_subscription_identifiers(params['_'])
        return tuple(params[x] for x in id_kwargs)
    
    def parse_id_tuple(self, id_tuple):
        id_kwargs = self._fetch_subscription_identifiers(id_tuple[0])
        return {k: id_tuple[i] for i,k in enumerate(id_kwargs)}
        
        
    def get_subscription_id_tuple(self, params_or_uid):
        if hasattr(params_or_uid, '__iter__') and not isinstance(params_or_uid, (dict,str)):
            return tuple(params_or_uid)
        elif isinstance(params_or_uid, dict):
            return self.create_id_tuple(params_or_uid)
        else: 
            return self._get_subscription_id_tuple_by_uid(params_or_uid)
    
    
    def _get_subscription_id_tuple_by_uid(self, uid):
        try: 
            return next(s.id_tuple for s in self.wc.sh.subscriptions
                         if s.uid == uid)
        except StopIteration:
            raise ValueError('Unregistered uid: {}'.format(uid))
        
        
    def _fetch_subscription_identifiers(self, channel):
        inf = self.channels[channel]
        identifying = inf.get('identifying')
        id_kwargs = ('_',) + tuple(inf['required'] if identifying is None 
                                   else inf['identifying'])
        return id_kwargs
    
    def verify_has(self, channel, *args, throw=True):
        keys = (channel,) + args
        cur = self.wc.has
        answer = False
        exceeded = False
        
        for i,key in enumerate(keys):
            if not isinstance(cur,dict):
                exceeded = True
                break
            if hasattr(key,'__iter__') and not isinstance(key,str):
                key_list = key
                return all(self.verify_has(*keys[:i], k, *keys[i+1:], throw=throw) 
                           for k in key_list)
            cur = cur.get(key)
        
        if not exceeded:
            if isinstance(cur,dict) and '_' in cur:
                cur = cur['_']
            # True if dict contains items
            answer = bool(cur)
        
        if not throw:
            return answer
        elif answer:
            return True
        else:
            raise ValueError('Does not have {}'.format(keys))
        

    def has_got(self, channel, *args):
        return self.verify_has(channel, *args, throw=False)
    
    
    def verify_input(self, params):
        inf = self.channels[params['_']]
        missing = [x for x in inf['required'] if x not in params]
        if missing:
            raise KeyError('Missing arguments: {}'.format(missing))
        
    
    def fetch_connection_config(self, channel, params):
        profile = self.get_value(channel, 'connection_profile')
        converter = self.get_value(channel, 'cnx_params_converter')
        if converter is not None:
            params = converter(params.copy())
            
        funcs = dict.fromkeys(['handle', 'on_activate', 'ping', 'extra_headers'])
        _sets = {'extra_headers': 'channel'}
        _as_list = ['handle','on_activate']
        
        for name in funcs:
            set = _sets.get(name, 'cnx')
            if set == 'cnx':
                var = self.get_value(profile, name, set=set)
            else:
                var = self.get_value(channel, name, set=set)
            if var is None:
                continue
            if isinstance(var, str) or not hasattr(var, '__iter__'):
                var = [var]
            var = [self.wc.ip.interpret_variable(v) for v in var]
            if name not in _as_list:
                var = var[0]
            funcs[name] = var
            
        return dict(
            url = self.get_value(channel, 'url_factory').copy(params),
            **funcs,
            **{
               x: self.get_value(profile, x, set='cnx')
                for x in ['signalr', 'hub_name', 'hub_methods', 'socketio', 'event_names',
                          'reconnect_try_interval', 'connect_timeout', 'recv_timeout', 
                          'ping_interval', 'ping_after', 'ping_as_message',
                          'ping_timeout', 'poll_interval', 'rate_limit', 
                          'throttle_logging_level',]
            }, #queue_maxsizes = self.wc.queue_maxsizes,
               recv_queue = self.wc.tp.recv_queue, 
               event_queue = self.wc.tp._event_queue,
               name_prefix = self.wc.name+'[cnx]',
               loop = self.wc._loop, 
               out_loop = self.wc.loop
        )
        
    def get_value(self, _, keywords, default=None, set='channel'):
        """:param set: 'channel' or 'cnx'"""
        return deep_get(self.srch_seq(_, set), keywords, return2=default)
    
    def has_merge_option(self, channel):
        return self.get_value(channel, 'merge_option', False)
    
    def has_unsub_option(self, channel):
        return self.get_value(channel, 'unsub_option', True)
    
    def has_fetch_data_on_sub_requirement(self, channel):
        return self.get_value(channel, 'fetch_data_on_sub', True)
    
    def has_delete_data_on_unsub_requirement(self, channel):
        return self.get_value(channel, 'delete_data_on_unsub', True)
    
    def get_merge_limit(self, channel):
        return self.get_value(channel, 'merge_limit', None)
    
    def get_default_subscription_state(self, channel):
        return self.get_value(channel, 'default_subscription_state', 0)
    
    def is_subscription(self, channel):
        type = self.get_value(channel, 'type', 'subscription')
        return type in (None,'sub','subscription')
    
    def srch_seq(self, _, set='channel'):
        return {'channel': self.channel_srch_seq,
                'cnx': self.cnx_srch_seq}[set](_)
                
    def channel_srch_seq(self, channel):
        return [self.wc.channels.get(channel,{}), self.wc.channel_defaults]
    
    def cnx_srch_seq(self, profile):
        return [self.wc.connection_profiles.get(profile,{}), self.wc.connection_defaults]
    
    @property
    def channels(self):
        return self.wc.channels
    @property
    def channel_defaults(self):
        return self.wc.channel_defaults
    @property
    def connection_profiles(self):
        return self.wc.connection_profiles
    @property
    def connection_defaults(self):
        return self.wc.connection_defaults
