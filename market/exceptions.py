class MarketException(Exception):
    pass


class OrderExpiredError(MarketException):
    pass


class NotEnoughFunds(MarketException):
    pass
