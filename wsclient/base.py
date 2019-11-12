from .conn_mgr import ConnectionManager
from .sub import SubscriptionHandler, Merged
from .interpreter import Interpreter
from .channel import ChannelsInfo
from .url import URLFactory
from .extend_attrs import WSMeta
from .transport import Transport

from fons.dict_ops import deep_update, deep_get
from fons.errors import TerminatedException
from fons.event import Event
from fons.reg import create_name
import fons.log as _log

import functools
import asyncio
import copy as _copy

logger,logger2,tlogger,tloggers,tlogger0 = _log.get_standard_5(__name__)

_WSCLIENT_NAMES = set()
_ASC = ''.join(chr(i) for i in range(128))
_ALNUM_ = ''.join(x for x in _ASC if x.isalnum()) + '_'
_ALPHA_ = ''.join(x for x in _ASC if x.isalpha()) + '_'
_ALNUM_DOT_ = _ALNUM_ + '.'
_quit_command = object()


class WSClient(metaclass=WSMeta):
    auth_defaults = {
        'required': None, #if False, .sign procedure will not be invoked even if channel's "is_private" is True
                          #if True, will not be evoked even if "is_private" is False
        'via_url': False, #if True, authentication will not be evoked (regardless of "required" nad "is_private")
        'takes_input': True, #whether or not .sign takes .encode() output as its first argument
        'each_time': False, #if True, then .sign procedure will be invoked each time when "is_private" is True
        'send_separately': False, #if True, the .sign() output is sent separately (followed by the initial message)
        'set_authenticated': None, #if True, then cnxi.authenticated will be set only after send_separately is performed (in .tp.send)
                                     # .sign is invoked by one of the listed channels; otherwise always
    }
    #Used on .subscribe_to_{x} , raises ValueError when doesn't have
    # {
    #    "x": True,
    #    "y": {"_": False, "yz": True}
    # }
    has = {}
    
    channel_defaults = {
        #If channel's url is None, this will be used (may be a function):
        # final_url = (url / url()).format(**self.url_compontents)
        # (wrapped by .cis.wrap_url(url))
        'url': None,
        'connection_profile': None,
        #Whether or not .tp.send actually sends something to the cnx socket
        # (as opposed to just connecting)
        'send': True,
        'cnx_params_converter': None,
        'merge_option': False,
        'merge_limit': None, #>=2 for merge support
        'merge_index': None, #by default 1
        #if False, .remove_subscription raises WSError on that channel
        'unsub_option': True,
        #Fetch necessary subscription related data shortly after 
        # subscription becomes active
        'fetch_data_on_sub': True,
        #To ensure that the user won't be using out-dated data
        # after unsubbing (unsub might also occur due to websocket crash)
        'delete_data_on_unsub': True,
        #change it to 1 if subscription states are not updated by server
        'default_subscription_state': 0,
        #activate subscription when receives acknowledgement
        #set to "on_cnx_activation" for the sub to be activated when its cnx is activated
        'auto_activate': True,
    }
    #If "is_private" is set to True, .sign() is called on the specific output to server

    # {
    #    "channel": {
    #        # all listed on channel_defaults +
    #        "required": [],
    #        "identifiers": None,
    #        "is_private": False,
    # }
    channels = {}

    max_total_subscriptions = None
    
    connection_defaults = {
        #change hub_name and hub_methods only if signalr is set to True
        'signalr': False,
        'hub_name': None,
        #it is necessary to list *all* methods to avoid KeyError
        #during signalr_aio message handling
        'hub_methods': [],
        'connect_timeout': None,
        'reconnect_try_interval': 30,
        'recv_timeout': None,
        #timeout for "send" response (if expected)
        'waiter_timeout': 5,
        #function that returns ping message to be sent to the socket
        'ping': None,
        #This must be overriden if .ping is used
        'ping_interval': None,
        'poll_interval': 0.05,
        'max_subscriptions': None,
        #the interval between pushing (sending) individual subscriptions to the server
        'subscription_push_rate_limit': None,
    }
    connection_profiles = {}
    
    url_components = {}
    #since socket interpreter may clog due to high CPU usage,
    # maximum lengths for the queues are set, and if full,
    # previous items are removed one-by-one to make room for new ones
    queue_maxsizes = {
        'event': 100,
        'received': 0,#1000,
        'send': 100,
    }
    
    #numbers are always added, but uppers/lowers can be decluded
    message_id_config = {
        'uppers': False,
        'lowers': True,
        'length': 14,
        'type': 'str', #if 'int', return type will be int
    }
    #e.g. "id" if response = {"id": x, ...}
    message_id_keyword = None

    __extend_attrs__ = [
        'auth_defaults',
        'channel_defaults',
        'channels',
        'connection_defaults',
        'connection_profiles',
        'has',
        'message_id_config',
        'queue_maxsizes',
        'url_components',
        #'api_attr_limits',
        #'orderbook_sends_bidAsk',
    ]
    __deepcopy_on_init__ = __extend_attrs__[:]
    
    #To be initiated during the creation of the class,
    # and in every subclass that has its own __properties__
    #property_name, attr_name, getter_enabled(<bool>), setter_enabled, deleter_enabled
    # (by default all 3 are True)
    __properties__ = [['channels_info','cis'],
                      ['connection_manager','cm'],
                      ['interpreter','ip'],
                      ['subscription_handler','sh'],
                      ['transport','tp'],]
    
    #whether or not socket .recv and .send run on a thread or not
    # (message handler runs on main thread)
    #possibly useful to avoid handler slowing down .recv, which
    # may cause some messages from the server to be missed
    #sockets_per_thread = None
    
    name_registry = _WSCLIENT_NAMES
    
    ChannelsInfo_cls = ChannelsInfo
    ConnectionManager_cls = ConnectionManager
    Interpreter_cls = Interpreter
    SubscriptionHandler_cls = SubscriptionHandler
    Transport_cls = Transport
    
    
    def __init__(self, config={}):
        for key in config:
            setattr(self, key, deep_update(getattr(self, key, None), config[key], copy=True))
        for key in self.__deepcopy_on_init__:
            if key in config or not hasattr(self,key): continue
            setattr(self, key, _copy.deepcopy(getattr(self,key)))
            
        self.name = create_name(getattr(self,'name',None), self.__class__.__name__, self.name_registry)

        if getattr(self,'loop',None) is None:
            self.loop = asyncio.get_event_loop()
            
        self.tp = self.Transport_cls(self)
        self._loop = self.tp._thread._loop
        
        self.cis = self.ChannelsInfo_cls(self)
        self.sh = self.SubscriptionHandler_cls(self)
        self.ip = self.Interpreter_cls(self)
        self.cm = self.ConnectionManager_cls(self)
        
        self._closed = False
        
        for ch_values in list(self.channels.values()) + [self.channel_defaults]:
            if ch_values.get('url') is not None:
                ch_values['url_factory'] = URLFactory(self, ch_values['url'])
            cpc = ch_values.get('cnx_params_converter')
            if isinstance(cpc, str):
                if cpc.startswith('m$'): cpc = cpc[2:]
                ch_values['cnx_params_converter'] = getattr(self, cpc)
        
        if getattr(self,'subscriptions',None) is not None:
            for params in self.subscriptions:
                self.sh.add_subscription(params)
        
        #self.start.__doc__ = self.tp.start.__doc__    
        #self.send.__doc__ = self.tp.send.__doc__
        
        
    def start(self):
        return asyncio.ensure_future(self.tp.start(), loop=self.loop)
    
    
    def on_start(self):
        """Overwrite this method. May be asynchronous."""
    
    
    def send(self, params, wait=False, id=None, cnx=None, sub=None):
        return asyncio.ensure_future(self.tp.send(params,wait,id,cnx,sub), loop=self.loop)
    
    
    def encode(self, request, sub=None):
        """Overwrite this method
           :param request: a Request / Subscription object (containing .params)
           :param sub: if None, this is a non-subscription request. 
                       Otherwise True for subbing and False for unsubbing.
            The output will be sent to socket (but before that json encoding will be applied)"""
        raise NotImplementedError
    
    
    def sign(self, out={}):
        """Authenticates the output received from .encode(). Override for specific use."""
        return out.copy()
    
              
    def extract_message_id(self, R):
        """:type r: Response
           May want to override this method"""
        r = R.data
        if not isinstance(r,dict) or self.message_id_keyword is None:
            return None
        try: 
            return deep_get([r],self.message_id_keyword)
        except Exception as e:
            logger.error('{} - could not extract id: {}. r: {}.'.format(self.name, e, r))
            return None
        
    
    #async def create_connection_url(self, base_url, *args, **kw):
    #    return base_url
                
    def handle(self, R):
        """:type R: Response
          Override this for specific socket response handling. 
          async form is also accepted. Note that if signalr is enabled,
          R.data = raw_data (dict), not raw_data['M']['A'] (that is sent to its hub handlers)."""
          
    
    def fetch_data(self, subscription, prev_state):
        """Override this method"""
        
        
    def delete_data(self, subscription, prev_state):
        """Override this method"""
        
        
    @staticmethod
    def merge(param_arr):
        if isinstance(param_arr, (str, dict)) or not hasattr(param_arr, '__iter__'):
            return param_arr
        return Merged(param_arr)
    
    @staticmethod
    def is_param_merged(x):
        return isinstance(x, Merged)
    
        
    def broadcast_event(self, event_type, value):
        """Analogous to cnx.broadcast_event (here puts to user_event_queue)"""
        self.tp.station.broadcast('event', Event('0', event_type, value))
    
    def add_event_queue(self, queue=None, maxsize=None):
        return self.tp.add_queue('event', None, queue, maxsize)
    
    def add_recv_queue(self, queue=None, maxsize=None):
        return self.tp.add_queue('recv', None, queue, maxsize)
    
    def remove_event_queue(self, id_or_queue):
        return self.tp.remove_queue('event', id_or_queue)
        
    def remove_recv_queue(self, id_or_queue):
        return self.tp.remove_queue('recv', id_or_queue)
    
    
    async def listen(self, channel='event', queue_id=0, timeout=None):
        """Listen for queue (default is event queue)
           :param channel: "event" and "recv" channels are being sent to"""
        #event queue receives ("socket","empty"), ("active",1) and ("active",0) events
        # ("active" isn't related to cnx-s, but whether or not WSClient is running)
        await asyncio.wait_for(self.tp.station.get_queue(channel, queue_id).wait(), timeout)
        
        
    def is_active(self):
        return self.tp.station.get_event('active', 0, loop=0).is_set()
    
    
    async def wait_till_active(self, timeout=None):
        if self._closed:
            raise TerminatedException('{} is closed'.format(self.name))
        #self.station._print()
        #print({'loop': id(self.loop), 'out_loop': id(self.out_loop), 'context': id(asyncio.get_event_loop())})
        event = self.tp.station.get_event('active', 0)
        await asyncio.wait_for(event.wait(), timeout)
        
    
    def stop(self):
        running = self.tp.station.get_event('running', 0, loop=0)
        if running.is_set():
            self.cm.remove_all()
            self.tp.stop()
    
    def close(self):
        if self._closed:
            return
        self._closed = True
        self.stop()
        
        
    @property
    def has_got(self):
        return self.cis.has_got
    @property
    def has_merge_option(self):
        return self.cis.has_merge_option
    @property
    def get_merge_limit(self):
        return self.cis.get_merge_limit
    @property
    def has_unsub_option(self):
        return self.cis.has_unsub_option
    @property
    def verify_has(self):
        return self.cis.verify_has
    @property
    def subscribe_to(self):
        return self.sh.add_subscription
    @property
    def unsubscribe_to(self):
        return self.sh.remove_subscription
    @property
    def sub_to(self):
        return self.sh.add_subscription
    @property
    def unsub_to(self):
        return self.sh.remove_subscription
    @property
    def is_subscribed_to(self):
        return self.sh.is_subscribed_to
    @property
    def handle_subscription_ack(self):
        return self.sh.handle_subscription_ack
    @property
    def get_subscription(self):
        return self.sh.get_subscription
    @property
    def change_subscription_state(self):
        return self.sh.change_subscription_state
    @property
    def wait_till_subscription_active(self):
        return self.sh.wait_till_subscription_active
    @property
    def get_value(self):
        return self.cis.get_value
    #@property
    #def get_connection(self):
    #    return self.sh.find_available_connection
    @property
    def generate_message_id(self):
        return self.ip.generate_message_id
    @property
    def station(self):
        return self.tp.station
