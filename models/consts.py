import enum


class OrderType(enum.Enum):
    market_order = 'MarketOrder'
    limit_order = 'LimitOrder'


class DirectionType(enum.Enum):
    bid = 'Bid'
    ask = 'Ask'


class OrderStateType(enum.Enum):
    created = 'Created'
    in_market = 'InMarket'
    executed = 'Executed'
    cancelled = 'Cancelled'
