from fons.aio import call_via_loop, FonsEvent, FonsQueue
from fons.debug import trylog, wrap_trylog
from fons.dict_ops import deep_get
from fons.errors import TerminatedException
from fons.event import Station, force_put
from fons.threads import EliThread
import fons.log as _log

import sys
import asyncio
import copy as _copy
import functools
from collections import defaultdict

from .sub import Merged

logger, logger2, tlogger, tloggers, tlogger0 = _log.get_standard_5(__name__)

# Number of WSClient objects per thread. For infinite set to 0.
sockets_per_thread = 1
threads = set()
_quit_command = object()


class Transport:
    def __init__(self, wrapper):
        """:type wrapper: WSClient"""
        self.wc = wrapper

        # If param `_loop` was given in config, and a thread with such loop already exists,
        # share that thread
        _loop = getattr(self.wc, "_loop", None)
        thread = next((x for x in threads if _loop == x._loop), None)

        if thread is None:
            try:
                thread = min(threads, key=lambda x: x.socket_count)
            except ValueError:
                thread = None
            if (
                thread is not None
                and sockets_per_thread > 0
                and thread.socket_count >= sockets_per_thread
            ):
                thread = None

        if thread is not None:
            self._thread = thread
        else:
            if _loop is None:
                _loop = asyncio.new_event_loop()
            self._thread = EliThread(
                target=_loop.run_forever,
                loop=_loop,
                daemon=True,
                name="{}Thread".format(self.wc.name),
            )
            self._thread.socket_count = 0
            threads.add(self._thread)

        self.wc._loop = self._thread._loop

        # "_" beginning of an attr signifies that _thread._loop is used
        self._event_queue = FonsQueue(0, loop=self.wc._loop)
        self.recv_queue = FonsQueue(0, loop=self.wc.loop)
        self.send_queue = FonsQueue(
            self.wc.queue_maxsizes.get("send", 0), loop=self.wc.loop
        )
        self._send_event = FonsEvent(loop=self.wc._loop)

        self._event_queue.name = "{}[EQ]".format(self.wc.name)
        self.recv_queue.name = "{}[RQ]".format(self.wc.name)
        self.send_queue.name = "{}[SQ]".format(self.wc.name)

        # These are the two main queues:
        # event : receives ConnectionEvent from all (active) connections
        #         can also receive Event when evoking self.wc.broadcast_event(..)
        #         can be listened on using self.wc.listen()
        # recv : receives websocket inputs (wrapped with Response) from all (active) connections
        #        self.wc.handle receives from it
        # For adding more queues, use self.wc.add_event_queue / self.wc.add_recv_queue
        #
        self.user_event_queue = ueq = FonsQueue(
            self.wc.queue_maxsizes.get("event", 100), loop=self.wc.loop
        )
        ueq.name = "{}[UEQ]".format(self.wc.name)
        ueq.warn = False

        self.station = Station(
            [
                {
                    "channel": "event",
                    "id": 0,
                    "queue": ueq,
                    "event": False,
                    "loops": [0],
                },
                {
                    "channel": "recv",
                    "id": 0,
                    "queue": False,
                    "event": None,
                    "loops": [0],
                },
                {
                    "channel": "empty",
                    "id": 0,
                    "queue": False,
                    "event": None,
                    "loops": [0],
                },
                {
                    "channel": "running",
                    "id": 0,
                    "queue": False,
                    "event": None,
                    "loops": [0, -1],
                },
                {
                    "channel": "stopped",
                    "id": 0,
                    "queue": False,
                    "event": None,
                    "loops": [0, -1],
                },
                {
                    "channel": "active",
                    "id": 0,
                    "queue": False,
                    "event": None,
                    "loops": [0, -1],
                },
                {
                    "channel": "inactive",
                    "id": 0,
                    "queue": False,
                    "event": None,
                    "loops": [0, -1],
                },
            ],
            default_queue_size=100,
            loops={0: self.wc.loop, -1: self.wc._loop},
            name=self.wc.name + "[Station]",
        )

        self.station.broadcast("inactive")
        self.station.broadcast("stopped")
        self.waiters = {}
        self.futures = {}

    async def start(self):
        if "stop" in self.futures:
            try:
                await self.futures["stop"]
            except Exception:
                pass

        if "start" in self.futures:
            return await self.futures["start"]

        self.futures["start"] = asyncio.Future()
        f1 = f2 = None
        try:
            self.wc.log("starting")
            self.station.broadcast("running")
            self.station.broadcast("stopped", op="clear")

            (await self.wc.on_start()) if asyncio.iscoroutinefunction(
                self.wc.on_start
            ) else self.wc.on_start()

            _loop = self._thread._loop
            f1 = call_via_loop(wrap_trylog(self._main), loop=_loop, module="asyncio")
            f2 = call_via_loop(self.process_events, loop=_loop, module="asyncio")

            if not self._thread.is_alive():
                self._thread.start()

            await self.process_responses()

            await asyncio.wait([f1, f2])

        finally:
            if f1 is not None:
                f1.cancel()
            if f2 is not None:
                f2.cancel()
            if sys.exc_info()[1] is None:
                self.futures["start"].set_result(None)
            else:
                self.futures["start"].set_exception(sys.exc_info()[1])
            del self.futures["start"]
            self.station.broadcast("running", op="clear")
            self.station.broadcast("stopped")

    async def _main(self):
        # This runs in self._thread
        loop = self.wc.loop
        _loop = asyncio.get_event_loop()

        # .send() is rooted via .send_queue
        async def _send(future, params, wait, id, cnx, sub):
            """:type future: asyncio.Future"""
            try:
                r = await self.send(params, wait, id, cnx, sub)
            except Exception as e:
                self.wc.log(
                    "thread .send() error occurred: {}".format(repr(e)), "ERROR"
                )
                self.wc.log(e)
                loop.call_soon_threadsafe(functools.partial(future.set_exception, e))
            else:
                loop.call_soon_threadsafe(functools.partial(future.set_result, r))

        # receives updates from the queue, sends them.
        async def _fire_from_send_queue():
            while not self.wc._closed:
                # get from queue via the main loop
                fut = call_via_loop(
                    self.send_queue.get,
                    module="asyncio",
                    loop=loop,
                )
                item = await fut
                if item is _quit_command:
                    break
                future, params, wait, id, cnx, sub = item
                asyncio.ensure_future(_send(future, params, wait, id, cnx, sub))

        # Start firing right now (allow user initiated sending) before pushing
        # the subs, as the .wc.handle method (which runs outside of the ._thread)
        # might need to respond to some of the messages received during the pushing
        fire = asyncio.ensure_future(_fire_from_send_queue())

        self.station.broadcast("active")
        self.station.broadcast("inactive", op="clear")
        self.wc.broadcast_event("active", 1)

        try:
            await call_via_loop(
                self.wc.wait_till_active, (5,), loop=self.wc.loop, module="asyncio"
            )
            await self.wc.wait_till_active(1)
            # Connections will be started when subscriptions are being pushed
            # (in .send method). Note that since this here is running in ._thread,
            # the subscription out messages are NOT sent via the queue.
            await self.wc.sh.push_subscriptions()
            await fire
        finally:
            fire.cancel()
            self.station.broadcast("active", op="clear")
            self.station.broadcast("inactive")
            self.wc.broadcast_event("active", 0)
            self.wc.log("stopped")

    async def process_responses(self):
        # Process received results from connections. This runs in the main loop (self.loop)
        self.wc.log2("starting response processing")
        iscoro = asyncio.iscoroutinefunction(self.wc.handle)
        recv = None

        while not self.wc._closed:
            poll_interval = self.wc.connection_defaults["poll_interval"]
            if recv is None:
                recv = asyncio.ensure_future(self.recv_queue.get())

            done, pending = await asyncio.wait([recv], timeout=poll_interval)
            if not recv.done():
                self.station.broadcast("empty")
                self.wc.broadcast_event("socket", "empty")
                continue

            R = recv.result()
            recv = None
            if R is _quit_command:
                break

            try:
                if iscoro:
                    await self.wc.handle(R)
                else:
                    self.wc.handle(R)
            except Exception as e:
                str_r = str(R.data)
                dots = "..." if len(str_r) > 200 else ""
                logger2.error(
                    "{} - {} while handling response: {}{}".format(
                        self.wc.name, e.__class__.__name__, str_r[:200], dots
                    )
                )
                logger.error(str_r)
                logger.exception(e)
            else:
                message_id = self.wc.extract_message_id(R)
                if message_id is not None:
                    errors = self.wc.extract_errors(R)
                    error = (
                        errors
                        if isinstance(errors, Exception)
                        else (errors[0] if len(errors) else None)
                    )
                    ftw = functools.partial(
                        self.forward_to_waiters,
                        message_id,
                        R,
                        error=error,
                        remove=True,
                        copy=1,
                    )
                    self.wc._loop.call_soon_threadsafe(functools.partial(trylog, ftw))
                self.station.broadcast("recv")

        if recv is not None:
            recv.cancel()

    async def process_events(self):
        # Process events from connections. This runs in self._thread
        self.wc.log2("starting event processing")
        activation_count = defaultdict(int)

        while not self.wc._closed:
            e = await self._event_queue.get()
            if e is _quit_command:
                break

            try:
                cnx = self.wc.cm.connections[e.id]
            except KeyError:
                continue

            if e.type == "socket" and e.data == 0:
                for s in self.wc.sh.subscriptions:
                    if s.cnx is cnx:
                        self.wc.sh.change_subscription_state(s, 0)
                try:
                    self.wc.cm.cnx_infs[cnx].authenticated = False
                except KeyError:
                    pass

            elif e.type == "socket" and e.data == 1:
                # Do not push on first notification,
                # as during that time *all* subscriptions are being pushed anyway
                pushed = None
                if activation_count[cnx]:
                    pushed = await self.wc.sh.push_subscriptions(cnx)

                for s in self.wc.sh.subscriptions:
                    if s.cnx is cnx:
                        v = self.wc.cis.get_value(s.channel, "auto_activate")
                        if v == "on_cnx_activation" and (pushed is None or s in pushed):
                            self.wc.sh.change_subscription_state(s, 1)

                activation_count[cnx] += 1

            elif e.type == "running" and e.data == 0:
                activation_count[cnx] = 0

    async def send(self, params, wait=False, id=None, cnx=None, sub=None):
        """
        :type params: dict, Request, Subscription
        :type cnx: Connection
        :param params:
          must contain '_' key; also accepts Subscription object
        :param wait:
          False : don't wait
          'default', True->'default', None, of type int/float: timeout to wait
          'return': doesn't wait, but returns the waiter
          if signalr/socketio is enabled then id system is probably not implementable (leave wait to False).
        :param id:
          custom id to be attached to the waiter. in that case user must forward the response to
          the waiter manually (e.g. through .handle)
          if channel sends multiple messages, id may be given as [id0, id1, ..]
        :param cnx:
          may be specified if params is dict, otherwise cnx of given request will be used
        :param sub:
          if subscription, True for subbing, False for unsubbing
        """
        if isinstance(params, dict):
            params = params.copy()
            output = self.wc.transform(params)
            if output is not None:
                params = output

        if isinstance(params, dict) and cnx is None:
            cnx = self.wc.sh.find_available_connection(params, create=True)

        rq = (
            Request(params, self.wc, cnx) if not isinstance(params, Request) else params
        )

        # Forward to thread
        if not self._is_in_thread_loop():
            fut = asyncio.Future()
            await self.send_queue.put((fut, rq, wait, id, cnx, sub))
            return await fut

        params = rq.params
        cnx = rq.cnx
        channel = rq.channel
        cnxi = self.wc.cm.cnx_infs[cnx]
        send = self.wc.cis.get_value(channel, "send")
        drop = self.wc.cis.get_value(channel, "drop_unused_connection", not send)

        if not cnx.is_running() and (sub is None or sub):
            try:
                cnx.start()
            except RuntimeError:
                pass
            await cnx.wait_till_active(cnx.connect_timeout)
        elif (sub is not None and not sub) and cnx.url and drop:
            # Due to param variance difference old connections would not satisfy
            # the new variance, and would remain unused
            # if rq.is_merger() and not any(_s.merger is not rq and _s.cnx is cnx
            #                               for _s in self.wc.sh.subscriptions):
            if not any(cnx is _s.cnx for _s in self.wc.sh.subscriptions):
                self.wc.cm.remove_connection(cnx, True)
            if cnx.is_running():
                await cnx.stop()

        wait = "default" if wait is True else wait
        add_waiter = wait is not False
        return_waiter = wait in (
            "return",
            "return-waiter",
            "return_waiter",
            "return waiter",
        )

        def _return():
            if not add_waiter or not return_waiter:
                return None
            else:
                f = asyncio.Future()
                f.set_result(None)
                return f

        if not cnx.url or not send:
            return _return()

        packs = self.wc.encode(rq, sub)
        # WSClient.encode should return `None` if this specific request is not meant to be sent
        if packs is None:
            return _return()

        if not isinstance(packs, Merged):
            packs = [packs]
        single = len(packs) == 1

        is_private = self.wc.cis.get_value(channel, "is_private")
        auth_seq = cnxi.auth_seq(channel)
        apply_to_packs = deep_get(auth_seq, "apply_to_packs")
        iscoro = asyncio.iscoroutinefunction(self.wc.sign)

        async def sign(_aInp):
            if iscoro:
                return await self.wc.sign(*_aInp)
            else:
                return self.wc.sign(*_aInp)

        async def send_pack(i):
            pck = packs[i]
            msg_id = None
            if isinstance(pck, tuple):
                out, msg_id = pck
            else:
                out = pck
            if id is not None:
                try:
                    msg_id = id[i] if not isinstance(id, (str, int)) else id
                except IndexError:
                    pass

            if add_waiter and msg_id is None:
                raise RuntimeError(
                    '{} - waiter requested but "message_id" was not '
                    "provided / returned by .encode(config)".format(self.wc.name)
                )

            async def do_auth(out):
                seq = cnxi.auth_seq(channel, i)
                auth_required = deep_get(seq, "is_required", return2=None)
                takes_input = deep_get(seq, "takes_input", return2=True)
                via_url = deep_get(seq, "via_url")
                each_time = deep_get(seq, "each_time")
                send_separately = deep_get(seq, "send_separately")
                set_authenticated = deep_get(seq, "set_authenticated", return2=True)

                self.wc.log2(
                    "do_auth || i: {i} params: {params}, is_private: {is_private} auth_required: {auth_required} "
                    "via_url: {via_url} each_time: {each_time} send_separately: {send_separately} "
                    "takes_input: {takes_input} set_authenticated: {set_authenticated} cnxi.authenticated: {authenticated}".format(
                        i=i,
                        params=params,
                        is_private=is_private,
                        auth_required=auth_required,
                        via_url=via_url,
                        each_time=each_time,
                        send_separately=send_separately,
                        takes_input=takes_input,
                        set_authenticated=set_authenticated,
                        authenticated=cnxi.authenticated,
                    )
                )

                if (
                    is_private and auth_required is None or auth_required
                ) and not via_url:
                    _aInp = [out] if takes_input else []
                    if each_time:
                        out = await sign(_aInp)
                    elif not cnxi.authenticated:
                        _aOut = await sign(_aInp)
                        if not send_separately:
                            out = _aOut
                        else:
                            await cnx.send(_aOut)

                        if set_authenticated:
                            cnxi.authenticated = True

                return out

            if apply_to_packs is None or i in apply_to_packs:
                out = await do_auth(out)

            await cnx.send(out)

            waiter = self.add_waiter(msg_id) if add_waiter else None
            if not add_waiter:
                return None
            elif return_waiter:
                return waiter
            else:
                _wait = (
                    wait
                    if wait != "default"
                    else self.wc.cis.get_value(channel, "waiter_timeout", set="cnx")
                )

                try:
                    return await self.wait_on_waiter(waiter, _wait)
                finally:
                    self.remove_waiter(waiter)

        r = []
        for i in range(len(packs)):
            r.append(await send_pack(i))

        return r[0] if single else r

    def forward_to_waiters(
        self, message_id, message, error=None, remove=True, copy=None
    ):
        try:
            waiters = self.waiters[message_id]
        except KeyError:
            return
        for i, waiter in enumerate(waiters):
            if (
                copy is True
                or isinstance(copy, int)
                and copy is not False
                and copy >= i
            ):
                if error is None:
                    message = _copy.deepcopy(message)
                else:
                    error = _copy.deepcopy(error)
            try:
                if error is None:
                    waiter.set_result(message)
                else:
                    waiter.set_exception(error)
            except asyncio.InvalidStateError:
                logger.error(
                    '{} - "{}"\'s waiter #{} result has already been set'.format(
                        self.wc.name, message_id, i
                    )
                )
        if remove:
            del self.waiters[message_id]

    def add_waiter(self, message_id):
        waiter = asyncio.Future()
        waiter.message_id = message_id
        try:
            l = self.waiters[message_id]
        except KeyError:
            l = self.waiters[message_id] = []
        l.append(waiter)
        return waiter

    def remove_waiter(self, waiter):
        try:
            self.waiters[waiter.message_id].remove(waiter)
        except (KeyError, ValueError):
            return None
        else:
            return True
        finally:
            if not len(self.waiters.get(waiter.message_id, [None])):
                del self.waiters[waiter.message_id]

    def has_waiter(self, message_id):
        return message_id in self.waiters and len(self.waiters[message_id])

    async def wait_on_waiter(self, waiter, timeout="default"):
        if timeout == "default":
            timeout = self.wc.connection_defaults["waiter_timeout"]
        self.wc.log2(
            "waiting on waiter {}, timeout: {}".format(waiter.message_id, timeout)
        )
        try:
            r = await asyncio.wait_for(waiter, timeout)
        except asyncio.TimeoutError as e:
            self.wc.log2(
                "timeout occurred on waiter {}".format(waiter.message_id), "ERROR"
            )
            raise e
        self.wc.log2("{} wait finished".format(waiter.message_id))
        return r

    def add_queue(self, channel, id=None, queue=None, maxsize=None, loop="out"):
        loops = (
            ([loop] if loop != "out" else self.loop)
            if loop is not None
            else asyncio.get_event_loop()
        )
        if maxsize is None:
            maxsize = self.wc.queue_maxsizes.get(channel, 0)
        # for cnx in self.wc.cm.connections.values():
        #     cnx.add_queue(channel, id, queue, maxsize, loops)
        return self.station.add_queue(channel, id, queue, maxsize, loops)

    def remove_queue(self, channel, queue_or_id, loop="out"):
        id = (
            queue_or_id
            if not isinstance(queue_or_id, asyncio.Event)
            else queue_or_id.id
        )
        loops = (
            ([loop] if loop != "out" else self.loop)
            if loop is not None
            else asyncio.get_event_loop()
        )
        # for cnx in self.wc.cm.connections.values():
        #     cnx.remove(channel, id, loops)
        return self.station.remove(channel, id, loops)

    def _is_in_thread_loop(self):
        return asyncio.get_event_loop() is self._thread._loop

    async def stop(self):
        if "stop" in self.futures:
            return await self.futures["stop"]

        self.futures["stop"] = asyncio.Future()

        try:
            self.wc.cm.remove_all()

            if "start" in self.futures:
                for attr in ("recv_queue", "send_queue", "_event_queue"):
                    queue = getattr(self, attr)
                    call_via_loop(force_put, (queue, _quit_command), loop=queue._loop)

                try:
                    await self.futures["start"]
                except Exception:
                    pass

        finally:
            if sys.exc_info()[1] is None:
                self.futures["stop"].set_result(None)
            else:
                self.futures["stop"].set_exception(sys.exc_info()[1])
            del self.futures["stop"]


class Request:
    def __init__(self, params, wrapper, cnx=None):
        """:type wrapper: WSClient
        :type cnx: Connection"""
        self.wc = wrapper
        self.cnx = cnx
        self.params = params.copy()
        self.id_tuple = self.wc.cis.create_id_tuple(params)
        self.channel = self.id_tuple[0]
        self.merger = None

    def push(self):
        return asyncio.ensure_future(self.wc.tp.send(self))

    def is_merged(self):
        return self.merger is not None

    def is_merger(self):
        return False

    def __eq__(self, other):
        if not isinstance(other, Request):
            return False
        return other.id_tuple == self.id_tuple and other.cnx == self.cnx

    def __hash__(self):
        return id(self)


def set_sockets_per_thread(value):
    global sockets_per_thread
    sockets_per_thread = value
