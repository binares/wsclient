from wsbuild.conn import Connection

import asyncio

# import json
import fons.log as _log

loop = asyncio.get_event_loop()


def test_conn():
    cnx = Connection("wss://api.hitbtc.com/api/2/ws", handle=lambda r: print(r))
    cnx.send(
        {
            "method": "subscribeTicker",
            "params": {"symbol": "BTCUSD"},
        }
    )
    loop.run_until_complete(cnx.start())


if __name__ == "__main__":
    _log.quick_logging(2)
    test_conn()
