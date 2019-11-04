class WSError(Exception):
    pass


class SubscriptionError(WSError):
    pass


class SubscriptionLimitExceeded(SubscriptionError):
    pass
