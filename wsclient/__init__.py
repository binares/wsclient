__version__ = '0.1.0'
__author__ = 'binares'

'''
    A framework for implementing websocket APIs.
    Subscribe and unsubscribe to channels. Authenticate. Rate limits. Subscription limits.
    Multiple asynchronous connections (sockets), in case 
        - each channel has its own url
        - subscription limit to a single socket is exceeded
'''

from .conn import Connection, ConnectionEvent
from .base import WSClient
from .sub import Merged
from .transport import set_sockets_per_thread
from .errors import (WSError, SubscriptionError, SubscriptionLimitExceeded)
