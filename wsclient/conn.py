import websockets
import signalr_aio_wsclient as signalr_aio
import socketio
import sys
import json
import asyncio
import inspect
import time
import datetime
dt = datetime.datetime
td = datetime.timedelta

from fons.aio import call_via_loop_afut
from fons.debug import (safeTry, safeAsyncTry)

from fons.event import (Station, Event, force_put,
                        empty_queue, create_name)
from fons.func import (async_limitcalls, get_arg_count)
from fons.sched import AsyncTicker
import fons.log as _log

logger,logger2,tlogger,tloggers,tlogger0 = _log.get_standard_5(__name__)


_CONNECTION_IDS = set()
_CONNECTION_NAMES = set()
_stop_command = object()
_heartbeat = object()


class Connection:
    def __init__(self, url, handle=None,*, on_activate=None,
                 signalr=False, hub_name=None, hub_methods=None,
                 socketio=False, event_names=None,
                 reconnect_try_interval=None, connect_timeout=None, recv_timeout=None, 
                 ping_interval=None, ping=None, ping_after=None, ping_timeout=5,
                 ping_as_message=False, pong=None, pong_as_message=True,
                 rate_limit=None, poll_interval=None,
                 queue_maxsizes={}, recv_queue=None, event_queue=None,
                 id=None, name_prefix=None, loop=None, out_loop=None, throttle_logging_level=0,
                 extra_headers=None, unpack_json=True, verbose=0):
        """
        :param url: str or (coroutine) function that returns str
        :param handle: (sync) function or list of functions (or None)
        :param on_activate: (a)sync function or list of functions (or None)
        :param out_loop: for station events and queues (recv [received] queue, event queue)
        :param extra_headers: (coroutine) function that returns dict
        :param unpack_json:
            True, False, 'force' (logs and skips the object if it it is not unpackable)
            ignored if  `socketio` or `signalr` is enabled
                            
        """
        if signalr and socketio:
            raise ValueError('`signalr` and `socketio` can\'t be simultaneously True')
        if ping_interval is not None and not ping_as_message and (signalr or socketio):
            which = 'signalr' if signalr else 'socketio'
            raise ValueError('Cannot send raw ping frames if `{}` is set to True. ' \
                             'You could enable `ping_as_message` instead.'.format(which))
        if pong and not pong_as_message and (signalr or socketio):
            which = 'signalr' if signalr else 'socketio'
            raise ValueError('Cannot send raw pong frames if `{}` is set to True. ' \
                             'You could enable `pong_as_message` instead.'.format(which))
        if isinstance(ping_as_message, str) and ping_as_message!='dump':
            raise ValueError(ping_as_message)
        if isinstance(pong_as_message, str) and pong_as_message!='dump':
            raise ValueError(pong_as_message)
        self.url = url
        self.extra_headers = extra_headers
        self.handle = handle
        self.on_activate = on_activate
        self.verbose = verbose
        #if id is None, returns free connection number (as str)
        self.id = create_name(id, None, registry=_CONNECTION_IDS)
        #default_name is used (and int added to that if already taken)
        default_name = '{}-{}'.format(self.__class__.__name__ if name_prefix is None 
                                      else name_prefix, self.id)
        self.name = create_name(None, default_name, _CONNECTION_NAMES, add_int='if_taken')
        
        self.socketio = socketio
        self.signalr = signalr
        self.hub_name = hub_name
        self.hub_methods = hub_methods if hub_methods is not None else []
        self.event_names = event_names if event_names is not None else []
        self.conn = None
        self.socket = None
        self.hub = None
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        q = next((x for x in (recv_queue,event_queue) if x is not None), None)
        if out_loop is None and q is not None:
            out_loop = q._loop
        self.out_loop = out_loop if out_loop is not None else self.loop
        
        self.connect_timeout = connect_timeout
        self.recv_timeout = recv_timeout
        self.reconnect_try_interval = reconnect_try_interval
        self.poll_interval = poll_interval
        self.throttle_logging_level = throttle_logging_level
        self._throttle_original = self._throttle
        self.rate_limit = rate_limit
        
        self.ping_func = ping
        self.ping_interval = ping_interval
        self.ping_after = ping_after
        self.ping_as_message = ping_as_message
        self.ping_timeout = ping_timeout
        self.ping_ticker = None
        self.pong_func = pong
        self.pong_as_message = pong_as_message
        
        self.unpack_json = unpack_json
        
        self.station = Station(loops = {-1: self.loop, 
                                         0: self.out_loop},
                               name = self.name+'[Station]')
       
        station_channels = ['started','active','stopped','_stop_initiated','empty','event','recv']
        for ch in station_channels:
            self.station.add_channel(ch)
        for ch in ['started','active','stopped','_stop_initiated','empty']:
            self.station.add_event(ch, id=0)
            
        def _add_q(ch, q_id, queue):
            maxsizes = {'event': 100}
            #only add to out_loop
            loops = [0] if queue is None else [queue._loop]
            self.station.add_queue(ch, q_id, queue,
                                   maxsize = queue_maxsizes.get(ch, maxsizes.get(ch,0)),
                                   loops = loops)
            
        for ch,q in [['recv', recv_queue],
                     ['event', event_queue]]:
            if isinstance(q,dict):
                for i,_q in q.items():
                    _add_q(ch,i,_q)
            elif hasattr(q,'__iter__'):
                for i,_q in enumerate(q):
                    _add_q(ch,i,_q)
            else:
                _add_q(ch,0,q)

        self._queues = {}
        self.futures = dict.fromkeys(['start', 'stop', 'ping', 'recv_loop', 
                                      'on_activate', 'signalr_wait'])
        self.connected_url = None
        self.connected_ts = None
        self.last_recv_ts = None
        self._last_ping_ts = None
        self._ignore_recv_ts = False
        self._stopped = False
    
    
    async def _connect(self):
        suffix = ' (socketio)' if self.socketio else (' (signalr)' if self.signalr else '')
        self.log("connecting{}".format(suffix))
        params = {}
        
        url = self.url
        if hasattr(self.url,'__call__'):
            url = url()
        if inspect.isawaitable(url):
            url = await url
        
        headers_from_url = {}
        if isinstance(url, dict):
            _headers = url.get('extra_headers')
            if _headers is not None:
                headers_from_url.update(_headers)
            url = url['url']
        url_msg = 'url: {}'.format(url) if url!='' else 'url empty'
        self.log(url_msg)
        
        extra_headers = self.extra_headers
        if hasattr(extra_headers,'__call__'):
            extra_headers = extra_headers()
        if inspect.isawaitable(extra_headers):
            extra_headers = await extra_headers
        
        if extra_headers is None:
            extra_headers = {}
        
        final_headers = dict(extra_headers, **headers_from_url)
        if final_headers:
            params['extra_headers'] = final_headers
        
        if not url:
            pass
        elif self.signalr:
            await self._connect_signalr(url)
        elif self.socketio:
            await self._connect_socketio(url)
        else:
            await self._connect_ordinary(url, params)
        
        self.log("connection established")
        self.connected_url = url
        self.connected_ts = time.time()
        #self.socket.settimeout(self.timeout)
    
    
    async def _connect_ordinary(self, url, params={}):
        self.conn = websockets.connect(url, **params)
        self.socket = await self.conn.__aenter__() # this also execs logging.basicConfig
        q = self._socket_recv_queue
        
        async def recv_loop():
            while True:
                try: r = await self.socket.recv()
                except websockets.ConnectionClosed as e:
                    force_put(q, e)
                    break
                else: force_put(q, r)

        self.futures['recv_loop'] = \
            asyncio.ensure_future(recv_loop())
    
    
    async def _connect_signalr(self, url):
        self.conn = signalr_aio.Connection(url, session=None)
        self.hub = self.conn.register_hub(self.hub_name)
        #All data received from server is handled by .handle,
        # hub specific handling is not implemented
        async def do_nothing(msg):
            pass
        for method in self.hub_methods:
            self.hub.client.on(method, do_nothing)
        #messages to .recv are forwarded via signal_recv_queue
        q = self._socket_recv_queue
        async def put_to_recv_queue(**data):
            await q.put(data)
            
        self.conn.received += put_to_recv_queue
        self.conn.start()
        
        signalr_fut = self.conn._Connection__transport._conn_handler
        await asyncio.wait([signalr_fut], timeout=0.5)
        if signalr_fut.done():
            await signalr_fut # raises exception if occurred
        
        async def wait_on_signalr_future():
            try:
                await signalr_fut
            except websockets.ConnectionClosed as e:
                self.log('signalr socket has crashed')
                await q.put(e)
            else:
                self.log('signalr socket has been closed')
                await q.put(websockets.ConnectionClosed(-1, 'signalr ws closed'))
                
        self.futures['signalr_wait'] = asyncio.ensure_future(wait_on_signalr_future())
    
    
    async def _connect_socketio(self, url):
        self.conn = sio = socketio.AsyncClient(reconnection=False)
        #messages to .recv are forwarded via signal_recv_queue
        q = self._socket_recv_queue
        connected = asyncio.Event()
        
        async def message(data):
            await q.put(data)
        
        for event in self.event_names:
            sio.on(event)(message)
        
        @sio.event
        async def connect():
            connected.set()
        
        @sio.event
        async def connect_error(*args):
            self.log('socketio encountered connect error')
            await q.put(websockets.ConnectionClosed(-1, 'socketio connect error'))
        
        # This isn't executed when user itself evokes sio.disconnect (or here Connection.stop())
        @sio.event
        async def disconnect():
            self.log('socketio connection has crashed')
            await q.put(websockets.ConnectionClosed(-1, 'socketio ws crashed'))
        
        await sio.connect(url, transports='websocket')
        await connected.wait()
    
    
    async def _activate(self, timeout=None):
        # Close any previous connection if by chance left open (though it shouldn't happen)
        await self._exit_conn() 
        # Renew the queue to ensure that we won't be receiving anything from previous sockets
        # TODO: set max size to this queue?
        self._socket_recv_queue = asyncio.Queue(loop=self.loop)
        self.conn = None
        fut = asyncio.ensure_future(self._connect())
        wait_started = time.time()
        is_exceeded = lambda: timeout is not None and time.time() - wait_started > timeout
        while not self._stopped and not is_exceeded() and not fut.done():
            await asyncio.wait([fut], timeout=0.1)
        to_occurred = is_exceeded()
        if fut.done():
            await fut # raise exception if occurred
        else:
            fut.cancel()
            await self._exit_conn()
            if to_occurred:
                raise asyncio.TimeoutError('{} - connect timeout occurred'.format(self.name))
        if self._stopped:
            return
        self.station.broadcast('active')
        self.broadcast_event('socket', 1)
        if self.on_activate is not None:
            on_activate = [self.on_activate] if not hasattr(self.on_activate, '__iter__') \
                             else self.on_activate
            for i,f in enumerate(on_activate):
                args = [self] if get_arg_count(f) else []
                key = 'on_activate{}'.format('_{}'.format(i) if i else '')
                self.futures[key] = call_via_loop_afut(f, args)
    
    
    async def _safe_activate(self, timeout=None):
        await safeAsyncTry(self._activate, (timeout,), attempts=True,
                           pause=self.reconnect_try_interval,
                           exit_on=self.station.get_event('_stop_initiated',0,loop=-1))
    
    
    def start(self):
        f = self.futures['start']
        if f is not None and not f.done():
            raise RuntimeError('{} is already running.'.format(self.name))
        self._stopped = False
        self.futures['start'] = f = \
            asyncio.ensure_future(self._start(), loop=self.loop)
        return f
    
    
    async def _start(self):
        fstop = self.futures['stop']
        if fstop is not None and not fstop.done():
            self.log2("waiting till 'stop' fut done")
            await fstop

        self.station.broadcast_multiple(
            [{'_': 'started', 'op': 'set'},
             {'_': 'stopped', 'op': 'clear'},
             {'_': '_stop_initiated', 'op': 'clear'},]
        )
        self.broadcast_event('running', 1)
        await self._safe_activate(self.connect_timeout)
        
        if not self._stopped and self.connected_url:
            self._start_ping_ticker()
        
        self._ignore_recv_ts = True
        recv = self._get_recv()
        try:
            while not self._stopped:
                try:
                    #asyncio.wait doesn't cancel the recv task when timeout occurs (.gather does)
                    done, pending = await asyncio.wait([recv], timeout=self.poll_interval)
                    if recv.done():
                        #raises ConnectionClosed if .conn was closed
                        result = recv.result()
                        if result is _heartbeat:
                            continue
                        elif result is _stop_command:
                            break
                        
                    self._verify_recv_timeout()

                    if not recv.done():
                        self.station.broadcast('empty', op='set')
                        self.broadcast_event('socket', 'empty')
                        continue
                    
                    self.last_recv_ts = time.time() 
                    self._ignore_recv_ts = False
                    r = self.decode_response(result)
                    is_ping = await self.pong(r)
                    self.station.broadcast('empty', op='clear')
                    self.broadcast_event('socket', 'recv')
                    if is_ping:
                        continue
                    
                except websockets.ConnectionClosed:
                    await self._on_websocket_error()
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    dots = '...' if len(result) > 200 else ''
                    logger2.error("{} - non json-decodable response: {}{}"\
                                  .format(self.name, result[:200], dots))
                    logger.error(result)
                    logger.exception(e)
                else:
                    if self.verbose and self.verbose >= 3:
                        self.log3('received:\n{}'.format(r))
                    if self.handle is not None:
                        handlers = self.handle if hasattr(self.handle, '__iter__') else [self.handle]
                        for handle in handlers:
                            handle(r)
                    response = Response(self.id, 'recv', r)
                    self.station.broadcast('recv', response)
                finally:
                    if recv.done():
                        recv = self._get_recv()
        except Exception as e:
            logger2.error("Error occurred in '{}': {}".format(self.name, repr(e)))
            logger.exception(e)
        finally:
            self.station.broadcast('active', op='clear')
            self.broadcast_event('socket', 0)
            #print('Leaving {}'.format(self.name))
            recv.cancel()
            for fn in ('recv_loop','signalr_wait'):
                if self.futures[fn] is not None:
                    self.futures[fn].cancel()
            self.stop()
            await self._exit_conn()
    
    
    def _start_ping_ticker(self):
        if self.ping_interval is None:
            return
        self.ping_ticker = \
            AsyncTicker(self.ping,
                        self.ping_interval,
                        keepalive={'pause':self.ping_interval/3},
                        loop=self.loop,
                        logging_level=0,
                        name='{}-Ping-Ticker'.format(self.name))
        self.futures['ping'] = asyncio.ensure_future(self.ping_ticker.loop())
    
    
    def decode_response(self, r):
        """If signalr enabled then r may contain byte strings,
           override this method to decode them."""
        if self.signalr:
            return r
        elif self.socketio:
            return r # json.loads(r['data'])
        elif self.unpack_json == 'force':
            return json.loads(r)
        elif self.unpack_json:
            try:
                return json.loads(r)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return r
        else:
            return r
    
    
    def _verify_recv_timeout(self):
        if self.connected_url and self.recv_timeout is not None and not self._ignore_recv_ts and \
                self.last_recv_ts is not None and time.time() - self.last_recv_ts > self.recv_timeout:
            self.log('recv timeout occurred. Reconnecting.', 'ERROR')
            asyncio.ensure_future(self._exit_conn(self.conn))
            #To force the current recv to complete itself (if not already done)
            self._socket_recv_queue.put_nowait(_heartbeat)
            raise websockets.ConnectionClosed(-2, 'recv timeout occurred')
    
    
    async def _on_websocket_error(self):
        self._ignore_recv_ts = True
        if self._stopped: return
        self.log("websocket has crashed. Reconnecting.", logger=logger2)
        self.station.broadcast('active', op='clear')
        self.broadcast_event('socket', 0)
        await self._safe_activate(self.connect_timeout)
    
    
    def _get_recv(self):
        #will cause some receptions to be lost, as the task wrapped around _socket.recv()
        # is cancelled when timeout occurs:
        #r = await asyncio.wait_for(self._socket.recv(),self.poll_interval)
        #----
        async def get_from_queue():
            r = await self._socket_recv_queue.get()
            if isinstance(r, websockets.ConnectionClosed):
                raise r
            return r
        return asyncio.ensure_future(get_from_queue())
    
    
    async def recv(self, timeout=None, queue_id=0, strip=True):
        queue = self.station.get_queue('recv', queue_id)
        r = asyncio.wait_for(await queue.get(), timeout)
        if strip: 
            r = r.data
        return r
    
    
    def send(self, message, dump=True, log=True):
        return call_via_loop_afut(self._send, (message, dump, log), loop=self.loop)
    
    
    async def _send(self, message, dump=True, log=True):
        await self.throttle()
        await self.wait_till_active(self.connect_timeout)
        if log:
            self.log('sending: {}'.format(message))
        if self.signalr:
            self.hub.server.invoke(*message)
        elif self.socketio:
            await self.conn.emit(*message)
        else:
            send_msg = json.dumps(message) if dump else message
            await self.socket.send(send_msg)
    
    
    async def ping(self):
        try:
            last_ts = max(self._last_ping_ts, self.last_recv_ts)
        except TypeError:
            last_ts = next((x for x in (self._last_ping_ts, self.last_recv_ts) if x), 0)
        
        if self.ping_after is None or time.time() > last_ts + self.ping_after:
            if self.ping_as_message:
                message = self.ping_func()
                self.log2('sending ping: {}'.format(message))
                # message will be json encoded
                await self.send(message, dump=(self.ping_as_message=='dump'), log=False)
            else:
                args = [self.ping_func()] if self.ping_func is not None else []
                self.log2('sending ping{}'.format(': '+str(args[0]) if args else ''))
                # message will be converted into bytes (must be str or bytes)
                try: await asyncio.wait_for(self.socket.ping(*args), self.ping_timeout)
                except asyncio.TimeoutError:
                    await self._socket_recv_queue.put(
                        websockets.ConnectionClosed(-3, 'Ping timeout occurred'))
                finally:
                    self._last_ping_ts = time.time()
    
     
    async def pong(self, r):
        if not self.pong_func:
            return
        message = self.pong_func(r)
        if message is None:
            return
        
        self.log2('received ping: {}. sending pong: {}'.format(r, message))
        if self.pong_as_message:
            await self.send(message, dump=(self.pong_as_message=='dump'), log=False)
        else:
            await self.socket.pong(message)
        
        return True # pong was successfully sent
    
    
    async def throttle(self):
        await self._throttle()
    
    #This will be wrapped by .rate_limit setter (property)
    async def _throttle(self):
        pass
    
    
    def is_running(self):
        return self.futures['start'] is not None and not self.futures['start'].done() and \
            self.station.get_event('started',0,loop=0).is_set()
    
    
    def is_active(self):
        return self.is_running() and self.station.get_event('active',0,loop=0).is_set()
    
    
    def is_connected(self):
        return self.is_active()
    
    
    async def wait_till_active(self, timeout=None):
        #self.station._print()
        #print({'loop': id(self.loop), 'out_loop': id(self.out_loop), 'context': id(asyncio.get_event_loop())})
        event = self.station.get_event('active',0)
        await asyncio.wait_for(event.wait(), timeout)
    
    @property
    def wait_till_connected(self):
        return self.wait_till_active
    
    @property
    def add_receptor(self):
        return self.station.add
    @property
    def remove_receptor(self):
        return self.station.remove
    
    def add_queue(self, id, channel='recv', queue=None, maxsize=0, loop='out'):
        loops = ([loop] if loop!='out' else self.out_loop) if loop is not None else asyncio.get_event_loop()
        return self.station.add_queue(channel,id,queue,maxsize,loops)
    
    def remove_queue(self, id, channel='recv', loop='out'):
        loops = ([loop] if loop!='out' else self.out_loop) if loop is not None else asyncio.get_event_loop()
        return self.station.remove(channel,id,loops)

    def broadcast_event(self, event_type, value):
        self.station.broadcast('event', ConnectionEvent(self.id, event_type, value))
    
    
    def stop(self):
        f = self.futures['stop']
        if f is not None and not f.done():
            return f
        self.futures['stop'] = f = \
            asyncio.ensure_future(self._stop(), loop=self.loop)  
        return f
    
    async def _stop(self):
        #f = self.futures['stop']
        #if f is not None and not f.done():
        #    return
        self._stopped = True
        self.station.broadcast('_stop_initiated')
        if self.conn is None: 
            return
        fstart = self.futures['start']
        fping = self.futures['ping']
        self.log2('stopping')
        await self._socket_recv_queue.put(_stop_command)
        if self.ping_ticker is not None:
            await self.ping_ticker.close()
            if not fping.done():
                self.log2('waiting on "ping" future')
                await fping
        self.log2('closing socket')
        await self._exit_conn()
        if not fstart.done():
            self.log2('waiting on "start" future')
            await fstart
        self.conn = None
        self.hub = None
        self.socket = None
        empty_queue(self._socket_recv_queue)
        self.station.broadcast_multiple(
            [{'_': 'stopped', 'op': 'set'},
             {'_': 'started', 'op': 'clear'},]
        )
        self.broadcast_event('running', 0)
        self.log('stopped')
        
        
    async def _exit_conn(self, conn=None):
        conn = conn if conn is not None else self.conn
        if conn is None:
            pass
        elif self.signalr:
            safeTry(conn.close)
            #self.hub = None
        elif self.socketio:
            await safeAsyncTry(conn.disconnect)
            # this must be done here as socketio has not callback on user initiated disconnect
            await self._socket_recv_queue.put(websockets.ConnectionClosed(-1, 'socketio ws closed'))
        else:
            if hasattr(conn, 'ws_client'):
                try: await conn.__aexit__(*sys.exc_info())
                except Exception as e: logger.exception(e)
            #self.socket = None

        
    @property
    def rate_limit(self):
        return self._rate_limit
    
    @rate_limit.setter
    def rate_limit(self, value):
        _value = value
        if value is None:
            self._throttle = self._throttle_original
        else:
            value = (value, 1) if not hasattr(value,'__iter__') else tuple(value)
            if len(value) != 2: 
                raise ValueError(_value)
            self._throttle = \
                async_limitcalls(*value, 'sleep', 
                                 logging_level=self.throttle_logging_level, 
                                 retain_order=True,
                                 loop=self.loop, 
                                 f=self._throttle)
        self._rate_limit = value
    
    
    def _log(self, msg, level='DEBUG', logger=logger, add_name=True):
        if not isinstance(msg, Exception):
            if add_name:
                msg = '{} - {}'.format(self.name, msg)
            logger.log(_log.level_to_int(level), msg)
        else:
            logger.exception(msg)
    
    
    def log(self, msg, level='DEBUG', logger=logger, add_name=True):
        if self.verbose and self.verbose >= 1:
            self._log(msg, level, logger, add_name)
    
    
    def log2(self, msg, level='DEBUG', logger=logger, add_name=True):
        if self.verbose and self.verbose >= 2:
            self._log(msg, level, logger, add_name)
    
    
    def log3(self, msg, level='DEBUG', logger=logger, add_name=True):
        if self.verbose and self.verbose >= 3:
            self._log(msg, level, logger, add_name)
    
    
    def __str__(self):
        return self.name
        

class ConnectionEvent(Event):
    pass

class Response(Event):
    pass
