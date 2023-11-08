from fons.crypto import nonce as _nonce


class Interpreter:
    # Generate / decode message ids
    def __init__(self, wrapper):
        """:type wrapper: WSClient"""
        self.wc = wrapper

    def generate_message_id(self, uid, unsubscribe=False):
        c = self.wc.message["id"]["config"]
        kw = {x: y for x, y in c.items() if x in ("uppers", "lowers")}
        # if not c['numbers']: kw['set'] = 'alpha'
        as_int = c["type"] in ("int", int)
        set = "num" if as_int or not any(kw.values()) else "alnum"
        nonce = _nonce(c["length"] - 5, set, **kw)
        if uid is None:
            message_id = nonce + ("90000" if set == "num" else _nonce(5, "alpha", **kw))
        else:
            message_id = nonce + ("1" if not unsubscribe else "0") + str(uid)
        if as_int:
            message_id = int(message_id)
        return message_id

    @staticmethod
    def decode_message_id(message_id):
        if message_id is None:
            return (None, None)
        is_int = isinstance(message_id, int)
        str_id = str(message_id) if is_int else message_id
        if (
            len(str_id) < 5
            or str_id[-5] not in ("0", "1")
            or not all(x.isdigit() for x in str_id[-5:])
        ):
            return (None, None)
        uid = str_id[-4:]
        # 1: is subscribed; 0: unsubscribed
        status = 1 if str_id[-5] == "1" else 0
        return (uid, status)

    @staticmethod
    def extend(d, extra):
        return dict(d, **{k: v for k, v in extra.items() if k not in d})

    @staticmethod
    def comma_separate(items, f=None):
        if isinstance(items, str):
            items = [items]
        if f is not None:
            items = [f(x) for x in items]
        return ",".join(items)

    def interpret_variable(self, var):
        if isinstance(var, str):
            if var.startswith("m$"):
                var = var[2:]
            elif var.startswith("$"):
                var = var[1:]
            var = getattr(self.wc, var)
        return var
