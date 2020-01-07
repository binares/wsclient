from .conn_mgr import ConnectionManager
from .conn import Response
from .sub import SubscriptionHandler
from .merged import Merged
from .interpreter import Interpreter
from .channel import ChannelsInfo
from .errors import WSError
from .url import URLFactory
from .extend_attrs import WSMeta
from .transport import Transport

from fons.aio import call_via_loop_afut
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
        'apply_to_packs': None, #to which packs (that .encode() returns) the following applies
                                #if None, then to all of them
                                # example: [0,2] applies to [*pck0*, pck1 ,*pck2*]
        'is_required': None, #if False, .sign procedure will not be invoked even if channel's "is_private" is True
                          #if True, will not be evoked even if "is_private" is False
        'via_url': False, #if True, authentication will not be evoked (regardless of "required" and "is_private")
        'takes_input': True, #whether or not .sign takes .encode() output as its first argument
        'each_time': False, #if True, then .sign procedure will be invoked each time when "is_private" is True
                            #otherwise only if connection has not been authenticated yet (cnxi.authenticated)
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
        # If channel's url is None, this will be used (may be a function):
        #  final_url = (url / url()).format(**self.url_compontents)
        #  (wrapped by .cis.wrap_url(url))
        'url': None,
        'connection_profile': None,
        # Whether or not .tp.send actually sends something to the cnx socket
        # (as opposed to just connecting)
        'send': True,
        'cnx_params_converter': None,
        'merge_option': False,
        'merge_limit': None, #>=2 for merge support
        'merge_index': None, #by default 1
        # if False, .remove_subscription raises WSError on that channel
        'unsub_option': True,
        # Fetch necessary subscription related data shortly after 
        # subscription becomes active
        'fetch_data_on_sub': True,
        # To ensure that the user won't be using out-dated data
        # after unsubbing (unsub might also occur due to websocket crash)
        'delete_data_on_unsub': True,
        # change it to 1 if subscription states are not updated by server
        'default_subscription_state': 0,
        # Activate subscription when receives acknowledgement.
        # Set it to "on_cnx_activation" for the sub to be activated after its cnx is activated
        'auto_activate': True,
    }
    # If "is_private" is set to True, .sign() is called on the specific output to server

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
        # change hub_name and hub_methods only if signalr is set to True
        'signalr': False,
        'hub_name': None,
        # it is necessary to list *all* methods to avoid KeyError
        # during signalr_aio message handling
        'hub_methods': [],
        'connect_timeout': None,
        'reconnect_try_interval': 30,
        # if current_time > last_message_recived_timestamp + recv_timeout,
        # it assumes that the connection has been lost, and force crashes-reconnects
        # the socket
        'recv_timeout': None, #seconds
        # timeout for "send" response (if expected)
        'waiter_timeout': 5,
        # function that returns ping message to be sent to the socket
        # if it is a method of subclass, can be given as 'm$method_name_here'
        # if 'ping_as_message' == False, this may be left to None
        'ping': None,
        # This must be overriden to activate the ping utility
        'ping_interval': None,
        # if specified, ping ticker only sends the message if
        # current_time > last_message_received_timestamp + ping_after
        'ping_after': None, #in seconds
        # instead sending as raw bytes (via .ping of connection's socket)
        # sends the ping as an ordinary message (json decodes and sends it)
        'ping_as_message': False,
        # Only applies if 'ping_as_message' == False, force crashes-reconnects
        # the socket if no pong frame is received within that time
        'ping_timeout': 5,
        'poll_interval': 0.05,
        'max_subscriptions': None,
        # the interval between pushing (sending) individual subscriptions to the server
        'subscription_push_rate_limit': None,
    }
    connection_profiles = {}
    
    url_components = {}
    
    # Since socket interpreter may clog due to high CPU usage,
    # maximum lengths for the queues are set, and if full,
    # previous items are removed one-by-one to make room for new ones
    queue_maxsizes = {
        'event': 100,
        'received': 0,#1000,
        'send': 100,
    }
    
    message = {
        'id': {
            #numbers are always added, but uppers/lowers can be decluded
            'config': {
                'uppers': False,
                'lowers': True,
                'length': 14,
                'type': 'str', #if 'int', return type will be int
            },
            #e.g. "id" if response = {"id": x, ...}; may be a function
            'key':  None,
        },
        'error': {
            # error is present if error_msg=deep_get([r], keyword) is in .exceptions  
            # may be a function that extracts the msg
            'key': None,
            # extracted from response that you feed to the exception
            'args_keys': [],
        },
    }
    
    # Code/message: exception class
    exceptions = {
        '__default__': WSError,
    }
    
    # WSClient's subsclasses borrow __extend_attrs__ and __deepcopy_on__init__ from their parents,
    # except the attrs that are prefixed with '-' (and plain '-' will omit all from parents)
    # Example:
    # ```
    # class SubWSClient(WSClient):
    #    auth_defaults = {'via_url': True}
    #    connection_defaults = {'ping_interval': 2, 'something': 'some_value}
    #    __extend_attrs__ = ['new_attr','-auth_defaults]
    #    __deepcopy_on_init__ = ['new_attr_2']
    #
    # >>> SubWSClient.__deepcopy_on_init__ == ['new_attr_2'] + WSClient.__deepcopy_on_init__
    # True
    # >>> new_extend_attrs = ['new_attr'] + WSClient.__extend_attrs__
    # >>> new_extend_attrs.remove('auth_defaults')
    # >>> SubWSClient.__extend_attrs__ == new_extend_attrs
    # True
    # >>> new_connection_defaults = dict(WSClient.connection_defaults, ping_interval=2, something='some_value'}
    # >>> SubWSClient.connection_defaults == new_connection_defaults
    # True
    # >>> SubWSClient.auth_defaults
    # {'via_url': True}

    # ```
    # Now all SubWSClients attrs that are listed in its __extend_attrs__ will deep extend
    # its parents corresponding attrs. And on __init__ all attrs listed in  __deepcopy_on__init
    # will be deepcopied during SubWSClient.__init__ (and assigned to the resulting object)
    __extend_attrs__ = [
        'auth_defaults',
        'channel_defaults',
        'channels',
        'connection_defaults',
        'connection_profiles',
        'exceptions',
        'has',
        'message',
        'queue_maxsizes',
        'url_components',
    ]
    __deepcopy_on_init__ = __extend_attrs__[:]
    
    # To be initiated during the creation of the class, and in every subclass that has its own __properties__.
    # :: property_name, attr_name, getter_enabled(<bool>), setter_enabled, deleter_enabled
    #    (by default all 3 are True)
    __properties__ = [['channels_info','cis'],
                      ['connection_manager','cm'],
                      ['interpreter','ip'],
                      ['subscription_handler','sh'],
                      ['transport','tp'],]
    
    # Connection.recv and Connection.send run on on its own thread (message handler runs on main thread).
    # Possibly useful to avoid handler slowing down .recv, which may cause some messages from the server to be missed.
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
        if self._closed:
            raise TerminatedException("'{}' is closed".format(self.name))
        
        if asyncio.get_event_loop() is self.loop:
            return asyncio.ensure_future(self.tp.start())
        else:
            return call_via_loop_afut(self.tp.start, loop=self.loop)
    
    
    def on_start(self):
        """Overwrite this method. May be asynchronous.
           Is executed as the first thing when .start() is awaited on."""
    
    
    def send(self, params, wait=False, id=None, cnx=None, sub=None):
        return asyncio.ensure_future(self.tp.send(params,wait,id,cnx,sub), loop=self.loop)
    
    
    def encode(self, request, sub=None):
        """Overwrite this method
           :param request: a Request / Subscription object (containing .params)
           :param sub: if None, this is a non-subscription request. 
                       Otherwise True for subbing and False for unsubbing.
           :returns: output               | (non-tuple) 
                     (output, message_id) | (tuple)
                     .merge([pck0, pck1, ...]) where `pck` is on of the two above
            The output will be sent to socket (but before that json encoding will be applied)"""
        raise NotImplementedError
    
    
    def sign(self, out={}):
        """Authenticates the output received from .encode(). Override for specific use."""
        return out.copy()
    
              
    def extract_message_id(self, R):
        """
        :type R: Response
        May want to override this method
        """
        r = R.data
        key = self.message['id']['key']
        if not isinstance(r, dict) or key is None:
            return None
        try:
            if hasattr(key, '__call__'):
                return key(r)
            else:
                return deep_get([r], key)
        except Exception as e:
            logger.error('{} - could not extract id: {}. r: {}.'.format(self.name, e, r))
            return None
    
    
    def extract_errors(self, R):
        """
        :type R: Response
        :returns: list of errors
        May want to override this method
        """
        r = R.data if isinstance(R, Response) else R
        key = self.message['error']['key']
        if not isinstance(r, dict) or key is None:
            return []
        
        try:
            if hasattr(key, '__call__'):
                msg = key(r)
            else:
                msg = deep_get([r], key)
            
            e_cls = self.exceptions.get(msg, self.exceptions['__default__'])
            args = self.create_error_args(r)
            if not args:
                args = [msg]
            
            errors = [e_cls(*args)]
        
        except Exception as e:
            logger.error('{} - exception occurred while extracting errors: {}. r: {}.' \
                         .format(self.name, repr(e), r))
            logger.exception(e)
            errors = []
        
        return errors
    
    
    def create_error_args(self, r):
        """
        :returns: arguments for initiating the error
        May want to override this method
        """
        args_keys = self.message['error']['args_keys']
        if args_keys is None:
            return []
        args = []
        for key in args_keys:
            if hasattr(key, '__call__'):
                args.append(key(r))
            else:
                args.append(deep_get([r], key))
        
        return args
    
    
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
        # ("active" isn't related to cnx-s, but whether or not WSClient has pushed its subscriptions
        #   and is ready to accept from send queue)
        await asyncio.wait_for(self.tp.station.get_queue(channel, queue_id).wait(), timeout)
    
    
    def is_running(self):
        """Set when .start() is awaited on. Cleared when .start() finishes."""
        return self.tp.station.get_event('running', 0, loop=0).is_set()
    
        
    def is_active(self):
        """Set after send queue starts firing. Cleared when the firing stops.
           I.e .is_active()==True means that .send method is currently usable."""
        return self.tp.station.get_event('active', 0, loop=0).is_set()
    
    
    async def wait_till_active(self, timeout=None):
        if self._closed:
            raise TerminatedException('{} is closed'.format(self.name))
        #self.station._print()
        #print({'loop': id(self.loop), 'out_loop': id(self.out_loop), 'context': id(asyncio.get_event_loop())})
        event = self.tp.station.get_event('active', 0)
        await asyncio.wait_for(event.wait(), timeout)
        
    
    def stop(self):
        if asyncio.get_event_loop() is self.loop:
            return asyncio.ensure_future(self.tp.stop())
        else:
            return call_via_loop_afut(self.tp.stop, loop=self.loop)
    
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
