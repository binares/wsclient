from fons.dict_ops import deep_get
from fons.event import Station
import fons.log as _log

from .conn import Connection
from .errors import ConnectionLimitExceeded

logger, logger2, tlogger, tloggers, tlogger0 = _log.get_standard_5(__name__)

# How to integrate both url and socket sub registration?
# When unsubbing remove it from cnxi's url_factory, which
# will then later .satisfies(sub), but return e.g. 'through_socket'
# which will signal to .send() to proceed with the through-socket
# procedure


class ConnectionManager:
    def __init__(self, wrapper):
        """:type wrapper: WSClient"""
        self.wc = wrapper
        self.connections = {}
        self.cnx_infs = {}
        self.station = Station(
            [{"channel": "all_active", "id": 0, "queue": False}],
            loops=[self.wc.loop],
            name=self.wc.name + "[CM][Station]",
        )

    def add_connection(self, config, start=False):
        cur_len = len(self.connections)
        if (
            self.wc.max_total_connections is not None
            and cur_len >= self.wc.max_total_connections
        ):
            raise ConnectionLimitExceeded(
                "{} - number of connections ({}) has reached its limit ({})".format(
                    self.wc.name, cur_len, self.wc.max_total_connections
                )
            )
        cnx = Connection(**config)
        self.connections[cnx.id] = cnx
        self.cnx_infs[cnx] = ConnectionInfo(self.wc, cnx)
        if start:
            cnx.start()
        return cnx

    def remove_connection(self, id, delete=False):
        cnx = None
        if not isinstance(id, Connection):
            try:
                cnx = self.connections[id]
            except KeyError:
                pass
        else:
            cnx = id
        if cnx is not None:
            cnx.stop()
            if delete and cnx.id in self.connections:
                self.wc.log("removing connection <{}>".format(cnx.id))
                del self.connections[cnx.id]
            if delete and cnx in self.cnx_infs:
                del self.cnx_infs[cnx]
        return cnx

    def remove_all(self):
        for id in list(self.connections.keys()):
            self.remove_connection(id)

    def delete_connection(self, cnx):
        try:
            del self.connections[cnx.id]
        except KeyError:
            pass

        try:
            del self.cnx_infs[cnx]
        except KeyError:
            pass

        if cnx.is_running():
            cnx.stop()


class ConnectionInfo:
    def __init__(self, wc, cnx):
        """:type wc: WSClient
        :type cnx: Connection"""
        self.wc = wc
        self.cnx = cnx
        self.max_subscriptions = (
            self.wc.max_subscriptions_per_connection
            if self.wc.max_subscriptions_per_connection is not None
            else 100 * 1000
        )
        self.authenticated = False

    def satisfies(self, params, url_factory=None):
        """:param url_factory: may provide params specific url_factory
        which shall yield True if cnx.url.params == url_factory.params
        (useful for reusing old connection for the exact same subscription,
         if the url directly provides stream (takes only one sub, enabled by connecting))
        """
        channel = params["_"]
        # send = self.wc.cis.get_value(channel,'send',True)
        # register_via = self.wc.cis.get_value(channel,'register_via','socket').lower()
        # methods = register_via.split(' ')
        # url_satisfied = True
        # if 'url' in methods:
        if url_factory is None:
            url_factory = self.wc.cis.get_value(channel, "url_factory")
        if self.cnx.url != url_factory:
            return False
        auth_satisfied = self.auth_satisfied(channel)
        if not auth_satisfied:
            return False
        return True

    def auth_satisfied(self, channel):
        is_private = self.wc.cis.get_value(channel, "is_private")
        seq = self.auth_seq(channel)
        auth_required = deep_get(seq, "required", return2=None)
        if (not is_private and auth_required is None) or (
            auth_required is not None and not auth_required
        ):
            return True
        via_url = deep_get(seq, "via_url")
        if via_url:
            return True
        each_time = deep_get(seq, "each_time")
        if each_time or self.authenticated:
            return True
        # One for authentication subscription, the other for current subscription
        elif self.free_subscription_slots >= 2:
            return True
        return False

    def is_private(self):
        pass

    def auth_seq(self, channel, i=None):
        auth = self.wc.cis.get_value(channel, "auth", {})
        defaults = self.wc.auth_defaults
        if i is None:
            return [auth, defaults]
        else:
            auth_i = auth.get(i, {})
            defaults_i = defaults.get(i, {})
            return [auth_i, auth, defaults_i, defaults]

    def get_auth_value(self, channel, *args, **kw):
        """Deep get auth value of channel"""
        return deep_get(self.auth_seq(channel), *args, **kw)

    @property
    def connection(self):
        return self.cnx

    @property
    def sub_count(self):
        return sum(s.cnx is self.cnx for s in self.wc.sh.subscriptions)

    @property
    def free_subscription_slots(self):
        return self.max_subscriptions - self.sub_count
