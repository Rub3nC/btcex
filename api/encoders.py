from sqlalchemy.orm.query import Query

from models.order import Order
from models.contract import Instrument


def order_encoder(o):
    assert isinstance(o, Order)
    return {
        'id': o.id,
        'created': o.created_at,
        'instrument': o.instrument.identifier,
        'price': o.price,
        'volume': o.volume,
        'bid_or_ask': o.direction,
        'executed': o.executed(),
    }


def orders_encoder(o):
    assert isinstance(o, Query)
    return [order_encoder(order) for order in o]


def instrument_encoder(o):
    assert isinstance(o, Instrument)
    return {
        'identifier': o.identifier,
        'type': o.instrument_type,
        'last_24h_volume': o.last_24h_volume(),
        'last_24h_avg_price': o.last_24h_avg_price(),
        'open_asks': o.open_asks(),
        'open_bids': o.open_bids(),
        'latest_executed_price': o.latest_executed_price(),
        'latest_executed_volume': o.latest_executed_volume(),
        'ask': o.current_ask(),
        'bid': o.current_bid(),
    }


def instruments_encoder(o):
    assert isinstance(o, Query)
    return [instrument_encoder(instrument) for instrument in o]
