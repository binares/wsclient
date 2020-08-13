import copy as _copy
import asyncio
import functools
import datetime
dt = datetime.datetime
td = datetime.timedelta

import fons.log as _log
from fons.event import Station
from fons.func import get_arg_count

from .errors import (SubscriptionError, SubscriptionLimitExceeded)
from .merged import Merged
from .transport import Request


logger,logger2,tlogger,tloggers,tlogger0 = _log.get_standard_5(__name__)


class SubscriptionHandler:
    def __init__(self, wrapper):
        """:type wrapper: WSClient"""
        self.wc = wrapper
        
        self.subscriptions = []
        
        #all uids are in str form and consist of 4 digits
        self.uids = dict.fromkeys(['0'*(4-len(str(i)))+str(i) for i in range(1,10000)],False)
        

    async def push_subscriptions(self, cnx=None):
        cnx_sfx = 'cnx <{}>'.format(cnx.id) if cnx is not None else 'all'
        self.wc.log('pushing {} subscriptions'.format(cnx_sfx))
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
            self.wc.log('pushing {} to cnx <{}>'.format(s, s.cnx.id))
            try:
                await s.push()
            except Exception as e:
                logger2.error('{} - could not push {} to cnx <{}>'.format(self.wc.name, s, s.cnx.id))
                logger.exception(e)
            else:
                pushed.append(s)
            if self.wc.subscription_push_rate_limit:
                await asyncio.sleep(self.wc.subscription_push_rate_limit)
        self.wc.log('{} subscriptions pushed'.format(cnx_sfx))
        
        return pushed
        
        
    def add_subscription(self, params, *, dependants=[]):
        """:param params: dict or id_tuple"""
        if hasattr(params, '__iter__') and not isinstance(params, dict):
            params = self.wc.cis.parse_id_tuple(params)
        else:
            params = params.copy()
        output = self.wc.transform(params)
        if output is not None:
            params = output
        channel = params['_']
        self.wc.cis.verify_has(channel)
        self.wc.cis.verify_input(params)
        id_tuple = self.wc.cis.create_id_tuple(params)
        merge = False
        
        if self.wc.cis.has_merge_option(channel):
            merge_index = self.wc.cis.get_value(channel, 'merge_index', 1)
            #if merge_index <= 1: raise ValueError(merge_index)
            if isinstance(id_tuple[merge_index], Merged):
                merge = True
        elif any(isinstance(x, Merged) for x in id_tuple): 
            raise ValueError("{} - channel '{}' doesn't accept merged params; got: {}".format(
                              self.wc.name, channel, params))
        
        count = 1
        merge_limit = self.wc.cis.get_merge_limit(channel)
        
        if merge:
            params_list = []
            identifiers = self.wc.cis._fetch_subscription_identifiers(channel)
            p_name = identifiers[merge_index]
            count = len(params[p_name])
            if not len(params[p_name]):
                raise ValueError("{} - got empty merged param: '{}'".format(self.wc.name, p_name))
            elif merge_limit is not None and count > merge_limit:
                raise ValueError("{} - param '{}' count > merge_limit [{} > {}]".format(
                                  self.wc.name, p_name, count, merge_limit))
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
                _s = self.get_subscription(_params)
                if not dependants:
                    _s.independently_called = True
                self.wc.log2('already subbed to: {}'.format(_s.id_tuple))
            else:
                not_subbed.append(_params)
                
        count = len(not_subbed)
        if not count:
            return None
            
        if free is not None and count > free:
            raise SubscriptionLimitExceeded
        
        on_creation = self.wc.cis.get_value(channel, 'on_creation')
        if isinstance(on_creation, str):
            on_creation = self.wc.ip.interpret_variable(on_creation)
        
        if on_creation is not None:
            _args = [_copy.deepcopy(params)] if get_arg_count(on_creation) else []
            on_creation(*_args)
        
        merge = count > 1
        if merge:
            new_params = _copy.deepcopy(params)
            new_params[p_name] = self.wc.merge([x[p_name] for x in not_subbed])
        else:
            new_params = not_subbed[0]
        
        cnx = self.find_available_connection(new_params, create=True, count=count)
        self.wc.log('{} available cnx <{}>'.format(params, cnx.id))
        subs = [] 
        
        for i,_params in enumerate(not_subbed):
            s = Subscription(_params, self.wc, cnx, dependants=dependants)
            self.subscriptions.append(s)
            subs.append(s)
        
        if merge:
            s = SubscriptionMerger(channel, self.wc, cnx, subs, dependants=dependants)
        
        dependencies = self.wc.get_dependencies(s)
        for dep in dependencies:
            if not self.is_subscribed_to(dep):
                self.wc.log('adding provider sub {} for {}'.format(dep, s.id_tuple))
                self.add_subscription(dep, dependants=[s])
        
        if self.wc.tp._thread.isAlive() and self.wc.is_active():
            return s.push()
        
        return True
    
    
    def remove_subscription(self, x):
        """:param x: dict, id_tuple, uid, Request, Subscription, SubscriptionMerger"""
        #For removing merged subscription, pass SubscriptionMerger instance
        if hasattr(x, '__iter__') and not isinstance(x, (dict,str,Request)):
            x = self.wc.cis.parse_id_tuple(x)
            
        if isinstance(x, dict):
            channel = x['_']
            self.wc.cis.verify_has(channel)
            self.wc.cis.verify_input(x)
        
        if not self.is_subscribed_to(x):
            return None
  
        s = self.get_subscription(x)
        
        if getattr(s,'merger',None) is not None:
            raise SubscriptionError("{} - '{}' cannot be removed because it has merger attached to it.".format(
                                    self.wc.name, s.id_tuple))
        if not self.wc.cis.has_unsub_option(s.channel):
            raise SubscriptionError("{} - '{}' doesn't support unsubscribing.".format(
                                    self.wc.name, s.channel))
        if s.dependants or s.is_merger() and any(_s.dependants for _s in s.subscriptions):
            raise SubscriptionError("{} - '{}' cannot be removed because it has dependants.".format(
                                    self.wc.name, s.id_tuple))

        self.release_uid(s.uid)
        if s.is_merger():
            self.release_uid(_s.uid for _s in s.subscriptions)
        self.change_subscription_state(s, 0)
        s.independently_called = False
        
        id_tuples = [s.id_tuple] if not s.is_merger() else [_s.id_tuple for _s in s.subscriptions]
        for id_tuple in id_tuples:
            try: s_index = next(_i for _i,_s in enumerate(self.subscriptions) if _s.id_tuple == id_tuple)
            except StopIteration: continue
            else: del self.subscriptions[s_index]
        
        providers = self.find_provider_subs(s)
        for p in providers:
            p.remove_dependant(s)
        
        for p in providers:
            if not p.dependants and not p.independently_called:
                try:
                    self.wc.log("removing provider sub {} of {}".format(p.id_tuple, s.id_tuple))
                    self.remove_subscription(p)
                except Exception as e:
                    logger.error("{} - could not remove provider sub {} of {}: {}".format(
                                 self.wc.name, p.id_tuple, s.id_tuple, repr(e)))
                    logger.exception(e)
        
        if self.wc.is_active():
            return s.unsub()
        
        #If no "send" is deployed on cnx, and cnx would NOT be deleted 
        # (after *compulsory* stopping in "no send" case, as that is the only way to stop its feed),
        # re-subscribing on the old cnx could cause delay due to the old's "stop" future not having completed yet
        #Thus to guarantee the fluency it's better to delete the cnx entirely
        send = self.wc.cis.get_value(s.channel,'send')
        #Cnx with param variance probably can't be reused
        #if s.is_merger() and not any(s.cnx is _s.cnx for _s in self.subscriptions):
        if s.cnx.url and not send and not any(s.cnx is _s.cnx for _s in self.subscriptions):
            self.wc.cm.remove_connection(s.cnx, delete=True)
        
        return True
    
    
    def get_total_free_subscription_slots(self):
        if self.wc.max_total_subscriptions is not None:
            return self.wc.max_total_subscriptions - len(self.subscriptions)
        return None
    
    
    def find_provider_subs(self, s):
        return [_s for _s in self.subscriptions if s in _s.dependants]
    
    
    def find_available_connection(self, params, create=False, count=1):
        channel = params['_']
        is_sub = self.wc.cis.is_subscription(channel)
        free_slots = self.get_total_free_subscription_slots()
        if is_sub and free_slots is not None and count > free_slots:
            raise SubscriptionError
        
        cnx = None
        cfg = self.wc.cis.fetch_connection_config(channel, params)
        url_factory = cfg['url']
        
        for _cnx,_cnxi in self.wc.cm.cnx_infs.items():
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
            cfg = self.wc.cis.fetch_connection_config(channel, params)
        cnx = self.wc.cm.add_connection(cfg)
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
        uid,status = self.wc.ip.decode_message_id(message_id)
        if uid is None:
            return
        try: s = self.get_subscription(uid)
        except ValueError: 
            self.wc.log2("uid doesn't exist: {}".format(uid), 'WARN')
            return
        auto_activate = self.wc.cis.get_value(s.channel, 'auto_activate')
        if status and auto_activate != 'on_cnx_activation' and auto_activate:
            self.change_subscription_state(uid, 1)
        elif not status:
            self.change_subscription_state(uid, 0)
    
    
    def change_subscription_state(self, x, state, cnx_active=False, provider_active=True):
        """
        :param cnx_active: if True, positive state can only be assigned if subscription's connection is active
        :param provider_active: -||- all subscription's provider subs are active
        """
        #TODO: force state to 0 if not self.is_running() ?
        s = self.get_subscription(x)
        prev_state = s.state
        if prev_state == state:
            return
        
        if state and cnx_active and (s.cnx is None or not s.cnx.is_active()):
            return
        
        if state and provider_active:
            providers = self.find_provider_subs(s)
            if not all(p.state for p in providers):
                return
        
        s.state = state
        
        if state != prev_state:
            self.wc.log(s)
        
        def data_ops(s):
            #Fetch necessary data before enabling subscription
            if state and self.wc.cis.has_fetch_data_on_sub_requirement(s.channel):
                self.wc.loop.call_soon_threadsafe(functools.partial(
                    self.wc.fetch_data, s, prev_state))
            #To make it absolutely sure that the user won't be
            # using out-dated data
            if not state and self.wc.cis.has_delete_data_on_unsub_requirement(s.channel):
                self.wc.loop.call_soon_threadsafe(functools.partial(
                    self.wc.delete_data, s, prev_state))
                
        subs = [s] if not s.is_merger() else s.subscriptions
        for _s in subs:
            data_ops(_s)
        
        # Dependent subscription is disabled automatically, but enabling is left to user side. 
        if not state:
            for dep in s.dependants:
                self.change_subscription_state(dep, 0)
    
    
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
        id_tuple = self.wc.cis.get_subscription_id_tuple(x)
        
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
        return self.wc.ip
    
    
class Subscription(Request):
    def __init__(self, params, wrapper, cnx, *, dependants=[]):
        """:type wrapper: WSClient
           :type cnx: Connection"""
        super().__init__(params, wrapper, cnx)
        
        self.uid = self.wc.sh.create_uid()
        self.station = Station( [{'channel': 'active', 'id': 0, 'queue': False, 'loops': [0,-1]},
                                 {'channel': 'inactive', 'id': 0, 'queue': False, 'loops': [0,-1]}],
                                loops={-1: self.wc._loop, 
                                        0: self.wc.loop})
        self.state = self.wc.cis.get_default_subscription_state(self.channel)
        self.dependants = []
        self.independently_called = not dependants # this can also be True if it does have dependants
        for s in dependants:
            self.add_dependant(s)
        
    def add_merger(self, merger):
        if self.is_active():
            raise RuntimeError("{} - {} merger can't be added while sub is active".format(
                                self.wc.name, self.id_tuple))
        merger.add(self)
        
    def remove_merger(self, merger):
        merger.remove(self)
    
    def add_dependant(self, s):
        if s not in self.dependants:
            self.dependants.append(s)
            return True
        return None
    
    def remove_dependant(self, s):
        if s in self.dependants:
            self.dependants.remove(s)
            return True
        return None
    
    def push(self):
        if self.merger is not None:
            return self.merger.push()
        return asyncio.ensure_future(self.wc.tp.send(self, sub=True))
        
    def unsub(self):
        if self.merger is not None:
            return self.merger.unsub(self)
        return asyncio.ensure_future(self.wc.tp.send(self, sub=False))
        
    def is_active(self):
        return bool(self.state) # `active/inactive` events aren't set immediately, use `state` instead
    
    def _is_active(self):
        return self.station.get_event('active', 0, loop=0).is_set()
    
    def _is_active2(self):
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
    def __init__(self, channel, wrapper, cnx, subs=[], *, dependants=[]):
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
        
        super().__init__(params, wrapper, cnx, dependants=dependants)
        
        for s in subs:
            self.add(s)
        
    def add(self, s):
        if s.wc is not self.wc:
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
