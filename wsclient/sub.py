import copy as _copy
import asyncio
import functools
import datetime
dt = datetime.datetime
td = datetime.timedelta

import fons.log as _log
from fons.event import Station

from .errors import (SubscriptionError, SubscriptionLimitExceeded)
from .transport import Request


logger,logger2,tlogger,tloggers,tlogger0 = _log.get_standard_5(__name__)


class SubscriptionHandler:
    def __init__(self, wrapper, *,
                 max_connections=None, max_subscriptions=None, max_subs_per_socket=None,
                 subscription_push_rate_limit=None):
        """:type wrapper: WSClient"""
        self.ww = wrapper
        self.max_connections = max_connections
        self.max_subscriptions = max_subscriptions
        self.max_subs_per_socket = max_subs_per_socket
        self.subscription_push_rate_limit = subscription_push_rate_limit
        
        self.subscriptions = []
        
        #all uids are in str form and consist of 4 digits
        self.uids = dict.fromkeys(['0'*(4-len(str(i)))+str(i) for i in range(1,10000)],False)
        

    async def push_subscriptions(self, cnx=None):
        cnx_id = cnx.id if cnx is not None else 'null'
        tlogger.debug('{} - pushing cnx <{}> subscriptions'.format(self.ww.name, cnx_id))
        mergers = set()
        pushed = []
        
        for s in self.subscriptions:
            if cnx is not None and s.cnx is not cnx:
                continue
            if s.merger is not None:
                if s.merger in mergers:
                    continue
                mergers.add(s.merger)
                s = s.merger
            tlogger0.debug('{} - pushing {} to cnx <{}>'.format(self.ww.name, s, s.cnx.id))
            try: await s.push()
            except Exception as e:
                logger2.error('{} - could not push {} to cnx <{}>'.format(self.ww.name, s, s.cnx.id))
                logger.exception(e)
            else: pushed.append(s)
            if self.subscription_push_rate_limit is not None:
                await asyncio.sleep(self.subscription_push_rate_limit)
        tlogger.debug('{} - subscriptions pushed'.format(self.ww.name))
        
        return pushed
        
        
    def add_subscription(self, params):
        """:param params: dict or id_tuple"""
        if hasattr(params, '__iter__') and not isinstance(params, dict):
            params = self.ww.cis.parse_id_tuple(params)
        channel = params['_']
        self.ww.cis.verify_has(channel)
        self.ww.cis.verify_input(params)
        id_tuple = self.ww.cis.create_id_tuple(params)
        merge = False
        
        if self.ww.cis.has_merge_option(channel):
            merge_index = self.ww.cis.get_value(channel, 'merge_index')
            if merge_index is None: merge_index = 1
            #elif merge_index <= 1: raise ValueError(merge_index)
            if isinstance(id_tuple[merge_index], Merged):
                merge = True
        elif any(isinstance(x, Merged) for x in id_tuple): 
            raise ValueError("{} - channel '{}' doesn't accept merged params; got: {}".format(
                              self.ww.name, channel, params))
        
        count = 1
        merge_limit = self.ww.cis.get_merge_limit(channel)
        
        if merge:
            params_list = []
            identifiers = self.ww.cis._fetch_subscription_identifiers(channel)
            p_name = identifiers[merge_index]
            count = len(params[p_name])
            if not len(params[p_name]):
                raise ValueError("{} - got empty merged param: '{}'".format(self.ww.name, p_name))
            elif merge_limit is not None and count > merge_limit:
                raise ValueError("{} - param '{}' count > merge_limit [{} > {}]".format(
                                  self.ww.name, p_name, count, merge_limit))
            for p_value in params[p_name]:
                new = _copy.deepcopy(params)
                new[p_name] = p_value
                params_list.append(new)
        else:
            params_list = [params]
        
        
        free = self.get_total_free_subscription_slots()
        not_subbed = []
    
        for _params in params_list:
            if self.is_subscribed_to(_params):
                _id_t = self.ww.cis.create_id_tuple(_params)
                tlogger0.debug('{} - already subbed to: {}'.format(self.ww.name, _id_t))
            else:
                not_subbed.append(_params)
                
        count = len(not_subbed)
        if not count:
            return None
            
        if free is not None and count > free:
            raise SubscriptionLimitExceeded
        
        merge = count > 1
        if merge:
            new_params = _copy.deepcopy(params)
            new_params[p_name] = self.ww.merge([x[p_name] for x in not_subbed])
        else:
            new_params = not_subbed[0]
        
        cnx = self.find_available_connection(new_params, create=True, count=count)
        tlogger0.debug('{} - {} available cnx <{}>'.format(self.ww.name, params, cnx.id))
        subs = [] 
        
        for i,_params in enumerate(not_subbed):
            s = Subscription(_params, self.ww, cnx)
            self.subscriptions.append(s)
            subs.append(s)
        
        if merge:
            s = SubscriptionMerger(channel, self.ww, cnx, subs)
            
        if self.ww.tp._thread.isAlive() and self.ww.is_active():
            tlogger0.debug('{} - ensuring sub {} push future.'.format(self.ww.name, s.id_tuple))
            return s.push()
        
        return True
    
    
    def remove_subscription(self, x):
        """:param x: dict, id_tuple, uid, Request, Subscription, SubscriptionMerger"""
        #For removing merged subscription, pass SubscriptionMerger instance
        if hasattr(x, '__iter__') and not isinstance(x, (dict,str,Request)):
            x = self.ww.cis.parse_id_tuple(x)
            
        if isinstance(x, dict):
            channel = x['_']
            self.ww.cis.verify_has(channel)
            self.ww.cis.verify_input(x)
        
        if not self.is_subscribed_to(x):
            return None
  
        s = self.get_subscription(x)
        
        if getattr(s,'merger',None) is not None:
            raise SubscriptionError("{} - '{}' cannot be removed because it has merger attached to it.".format(
                                    self.ww.name, s.id_tuple))
        if not self.ww.cis.has_unsub_option(s.channel):
            raise SubscriptionError("{} - '{}' doesn't support unsubscribing.".format(
                                    self.ww.name, s.channel))

        self.release_uid(s.uid)
        if s.is_merger():
            self.release_uid(_s.uid for _s in s.subscriptions)
        self.change_subscription_state(s, 0)
        
        id_tuples = [s.id_tuple] if not s.is_merger() else [_s.id_tuple for _s in s.subscriptions]
        for id_tuple in id_tuples:
            try: s_index = next(_i for _i,_s in enumerate(self.subscriptions) if _s.id_tuple == id_tuple)
            except StopIteration: continue
            else: del self.subscriptions[s_index]

        if self.ww.is_active():
            return s.unsub()
        
        #If no "send" is deployed on cnx, and cnx would NOT be deleted 
        # (after *compulsory* stopping in "no send" case, as that is the only way to stop its feed),
        # re-subscribing on the old cnx could cause delay due to the old's "stop" future not having completed yet
        #Thus to guarantee the fluency it's better to delete the cnx entirely
        send = self.ww.cis.get_value(channel,'send',True)
        #Cnx with param variance probably can't be reused
        #if s.is_merger() and not any(s.cnx is _s.cnx for _s in self.subscriptions):
        if not send and not any(s.cnx is _s.cnx for _s in self.subscriptions):
            self.ww.cm.remove_connection(s.cnx, delete=True)
        
        return True
    
    
    def get_total_free_subscription_slots(self):
        if self.max_subscriptions is None:
            return None
        else:
            return self.max_subscriptions - len(self.subscriptions)
        
        
    def find_available_connection(self, params, create=False, count=1):
        channel = params['_']
        is_sub = self.ww.cis.is_subscription(channel)
        free_slots = self.get_total_free_subscription_slots()
        if is_sub and free_slots is not None and count > free_slots:
            raise SubscriptionError
        
        cnx = None
        cfg = self.ww.cis.fetch_connection_config(channel, params)
        url_factory = cfg['url']
        
        for _cnx,_cnxi in self.ww.cm.cnx_infs.items():
            if is_sub:
                free_slots = _cnxi.free_subscription_slots
                if free_slots is not None and free_slots < count:
                    continue
            if _cnxi.satisfies(params, url_factory):
                cnx = _cnx
                
        if cnx is None and create: 
            cnx = self.create_connection(params, cfg)
        if cnx is None:
            raise SubscriptionError
        
        return cnx
    
    
    def create_connection(self, params, cfg=None):
        if cfg is None:
            channel = params['_']
            cfg = self.ww.cis.fetch_connection_config(channel, params)
        #cnx = Connection(**cfg)
        cnx = self.ww.cm.add_connection(cfg)
        return cnx
        
        
    def is_subscribed_to(self, x, active=None):
        """:param x: dict, id_tuple, uid, Request, Subscription, MergedSubscription"""
        if isinstance(x, Request):
            if x.is_merger():
                return any(self.is_subscribed_to(s, active) for s in x.subscriptions)
            x = x.id_tuple
        
        try: s = self.get_subscription(x)
        except ValueError:
            return False
        if active:
            return s.is_active()
        else:
            return True
        
        
    def handle_subscription_ack(self, message_id):
        #print(r)
        uid,status = self.ww.ip.decode_message_id(message_id)
        if uid is None:
            return
        try: s = self.get_subscription(uid)
        except ValueError: 
            logger.warn("{} - uid doesn't exist: {}".format(self.ww.name, uid))
            return
        auto_activate = self.ww.cis.get_value(s.channel, 'auto_activate')
        if status and auto_activate != 'on_cnx_activation' and auto_activate:
            self.change_subscription_state(uid, 1)
        elif not status:
            self.change_subscription_state(uid, 0)
    
    
    def change_subscription_state(self, x, state):
        #TODO: force state to 0 if not self.is_running() ?
        s = self.get_subscription(x)
        prev_state = s.state
        if prev_state == state:
            return
        
        s.state = state
        
        if state != prev_state:
            tlogger0.debug('{} - {}'.format(self.ww.name, s))
        
        def data_ops(s):
            #Fetch necessary data before enabling subscription
            if state and self.ww.cis.has_fetch_data_on_sub_requirement(s.channel):
                self.ww.loop.call_soon_threadsafe(functools.partial(
                    self.ww.fetch_data, s, prev_state))
            #To make it absolutely sure that the user won't be
            # using out-dated data
            if not state and self.ww.cis.has_delete_data_on_unsub_requirement(s.channel):
                self.ww.loop.call_soon_threadsafe(functools.partial(
                    self.ww.delete_data, s, prev_state))
                
        subs = [s] if not s.is_merger() else s.subscriptions
        for _s in subs:
            data_ops(_s)
            
            
    def create_uid(self, reserve=True):
        uid = next(x for x,v in self.uids.items() if not v)
        if reserve:
            self.uids[uid] = True
        return uid
    
    def release_uid(self, uid):
        self.uids[uid] = False
    

    def get_subscription(self, x):
        """:param x: dict, id_tuple, uid, Request, Subscription, MergedSubscription"""
        if isinstance(x, Subscription):
            return x
        if isinstance(x, Request):
            x = x.id_tuple
        id_tuple = self.ww.cis.get_subscription_id_tuple(x)
        
        s = next((_s for _s in self.subscriptions if _s.id_tuple == id_tuple), None)
        if s is None:
            raise ValueError('No subscription match for {}'.format(x))
        return s
        
    def get_subscription_state(self, x):
        return self.get_subscription(x).state

    def get_subscription_uid(self, x):
        return self.get_subscription(x).uid
    
    async def wait_till_subscription_active(self, x, timeout=None):
        s = self.get_subscription(x)
        await s.wait_till_active(timeout)
    
    @property
    def ip(self):
        return self.ww.ip
    
    
class Subscription(Request):
    def __init__(self, params, wrapper, cnx):
        """:type wrapper: WSClient
           :type cnx: Connection"""
        super().__init__(params, wrapper, cnx)
        
        self.uid = self.ww.sh.create_uid()
        self.station = Station( [{'channel': 'active', 'id': 0, 'queue': False, 'loops': [0,-1]},
                                 {'channel': 'inactive', 'id': 0, 'queue': False, 'loops': [0,-1]}],
                                loops={-1: self.ww._loop, 
                                        0: self.ww.loop})
        self.state = self.ww.cis.get_default_subscription_state(self.channel)
        
    def add_merger(self, merger):
        if self.is_active():
            raise RuntimeError("{} - {} merger can't be added while sub is active".format(
                                self.ww.name, self.id_tuple))
        merger.add(self)
        
    def remove_merger(self, merger):
        merger.remove(self)
    
    def push(self):
        if self.merger is not None:
            return self.merger.push()
        return asyncio.ensure_future(self.ww.tp.send(self, sub=True))
        
    def unsub(self):
        if self.merger is not None:
            return self.merger.unsub(self)
        return asyncio.ensure_future(self.ww.tp.send(self, sub=False))
        
    def is_active(self):
        return self.station.get_event('active', 0, loop=0).is_set()
    
    def is_active2(self):
        return not self.station.get_event('inactive', 0, loop=0).is_set()
    
    async def wait_till_active(self, timeout=None):
        await asyncio.wait_for(self.station.get_event('active', 0).wait(), timeout)
    
    def _set_state(self, value):
        self._state = value
        active_op = 'set' if value else 'clear'
        inactive_op = 'clear' if value else 'set'
        self.station.broadcast_multiple(
            [
                {'_': 'active', 'op': active_op},
                {'_': 'inactive', 'op': inactive_op},
            ]
        )

    @property
    def state(self):
        return self._state
    @state.setter
    def state(self, value):
        self._set_state(value)

    @property
    def sub(self):
        return self.push
            
    def __str__(self):
        return 'Sub({}, state={})'.format(self.id_tuple, self.state)
    
    
class SubscriptionMerger(Subscription):
    def __init__(self, channel, wrapper, cnx, subs=[]):
        """:type wrapper: WSClient
           :type cnx: Connection"""
        id_tuple_kw = wrapper.cis._fetch_subscription_identifiers(channel)
        merge_index = wrapper.cis.get_value(channel, 'merge_index')
        merge_index = 1 if merge_index is None else merge_index
        
        params = dict(_ = channel, 
                      **{k: (None if i+1 != merge_index else Merged())
                         for i,k in enumerate(id_tuple_kw[1:])}
                      )
        
        self.id_tuple_kw = id_tuple_kw
        self.merge_index = merge_index
        self.merge_param = id_tuple_kw[self.merge_index]
        self.subscriptions = []
        
        super().__init__(params, wrapper, cnx)
        
        for s in subs:
            self.add(s)
        
    def add(self, s):
        if s.ww is not self.ww:
            raise ValueError('Subscription has different WSClient attached')
        elif s.channel != self.channel:
            raise ValueError('Subscription has different channel attached - {}'.format(s.channel))
        elif s.cnx != self.cnx:
            raise ValueError('Subscription has different cnx attached')
        elif s.merger is not None and s.merger is not self:
            raise ValueError('Subscription has different merger already attached')
        s.merger = self

        if s not in self.subscriptions:
            self.subscriptions.append(s)
            self.params[self.mp] = Merged(self.params[self.mp] + (s.params[self.mp],))
            self.params.update({k:v for k,v in s.params.items() if k not in ('_',self.mp)})
            self.update_id_tuple()
            return True
        return None
            
    def remove(self, s):
        if s.merger is not None and s.merger is not self:
            raise ValueError('Subscription has different merger attached.')
        s.merger = None
        try: self.subscriptions.remove(s)
        except ValueError:
            return None
        self.params[self.mp] = Merged(x for x in self.params[self.mp] if x != s.params[self.mp])
        self.update_id_tuple()
        return True
    
    def add_merger(self, merger):
        raise NotImplementedError
    
    def remove_merger(self, merger):
        raise NotImplementedError
    
    def is_merged(self):
        return True
    
    def is_merger(self):
        return True
    
    def update_id_tuple(self):
        self.id_tuple = tuple(self.params[k] for k in self.id_tuple_kw)
    
    def _set_state(self, value):
        super()._set_state(value)
        for s in self.subscriptions:
            s._set_state(value)
            
    @property
    def mp(self):
        return self.merge_param
    
  
class Merged(tuple):
    pass
