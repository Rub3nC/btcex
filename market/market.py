import logging
from datetime import datetime

from market.exceptions import MarketException, OrderExpiredError
from models.consts import DirectionType, OrderType, OrderStateType
from models.order import Order, Transaction

logger = logging.getLogger(__file__)


def execute(session, first_order, second_order):
    if not isinstance(first_order, Order) or not isinstance(second_order, Order):
        logger.error('Both arguments are not Order instances')
        raise MarketException('Both arguments are not Order instances')

    if first_order.state != OrderStateType.in_market.value or second_order.state != OrderStateType.in_market.value:
        logger.error('At least one order is not in market ({}, {})'.format(first_order.id, second_order.id))
        raise MarketException('At least one order is not in market')

    if first_order.executed() or second_order.executed():
        logger.error('First or second order is already executed ({}, {}))'.format(first_order.id, second_order.id))
        raise MarketException('First or second order is already executed')

    def has_expired(order, now):
        if order.expires_in is not None and order.created_at + order.expires_in > now:
            return True
        return False

    now = datetime.now()
    if has_expired(first_order, now) or has_expired(second_order, now):
        logger.error('At least one order has expired ({}, {})'.format(first_order.id, second_order.id))
        raise OrderExpiredError('At least one order has expired')

    if first_order.direction == second_order.direction:
        logger.error('Orders have the same direction ({}, {})'.format(first_order.id, second_order.id))
        raise MarketException('Orders have the same direction')

    if first_order.contract != second_order.contract:
        logger.error('Orders have different contracts ({}, {})'.format(first_order.id, second_order.id))
        raise MarketException('Orders have different contracts')

    if first_order.price is None and second_order.price is None:
        logger.error('Both orders have no price specified ({}, {})'.format(first_order.id, second_order.id))
        raise MarketException('Orders have no price specified')

    volume = min([first_order.volume, second_order.volume])

    if first_order.created_at <= second_order.created_at:
        earliest_order, latest_order = first_order, second_order
    else:
        earliest_order, latest_order = second_order, first_order

    if earliest_order.price is None and latest_order.price is not None:
        price = latest_order.price
    elif earliest_order.price is not None and latest_order.price is None:
        price = earliest_order.price
    else:
        # Both prices are specified
        if earliest_order.direction == DirectionType.ask.value:
            price = max([earliest_order.price, latest_order.price])
        else:
            price = min([earliest_order.price, latest_order.price])

    def verify_price(order, price):
        if order.price is None:
            return True

        if order.direction == DirectionType.ask.value and order.price < price:
            return False
        elif order.direction == DirectionType.bid.value and order.price > price:
            return False
        else:
            return True

    if not verify_price(first_order, price) or not verify_price(second_order, price):
        logger.error('Tried to pay more / less than expected ({}, {})'.format(first_order.id, second_order.id))
        raise MarketException('Tried to pay more or less than expected')

    first_order.executed_at, second_order.executed_at = now, now
    first_order.state, second_order.state = OrderStateType.executed.value, OrderStateType.executed.value
    first_order_is_ask_order = bool(first_order.direction == DirectionType.ask.value)
    ask_order = first_order if first_order_is_ask_order else second_order
    bid_order = second_order if first_order_is_ask_order else first_order
    transaction = Transaction(contract=first_order.contract,
                              ask_order=ask_order,
                              bid_order=bid_order,
                              price=price,
                              volume=volume)
    session.add_all([first_order, second_order, transaction])

    try:
        if transaction.execute_trade(session):
            session.commit()
            logger.info('Executed orders {}, {}. ({})'.format(first_order.id, second_order.id, transaction.id))
            return transaction
    except MarketException as e:
        session.rollback()
        logger.warning('Did *not* execute orders {}, {}: {}'.format(first_order.id, second_order.id, str(e)))
        return


def put_order(session, order):
    if order.state != OrderStateType.created.value:
        logger.error('Order {} was not in state `created` but {}'.format(order.id, order.state))
        raise MarketException('Order not in state created')

    order.state = OrderStateType.in_market.value
    session.add(order)
    session.commit()
    logger.info('Order {} is now in state `in market`'.format(order.id))

    if order.direction == DirectionType.ask.value:
        reciprocal_direction = DirectionType.bid.value
    else:
        reciprocal_direction = DirectionType.ask.value

    candidate_orders = session.query(Order)\
        .filter(Order.contract == order.contract)\
        .filter(Order.direction == reciprocal_direction)\
        .filter(Order.state == OrderStateType.in_market.value)\
        .filter(Order.user != order.user)\
        .order_by(Order.id)

    if order.order_type == OrderType.market_order.value:
        candidate_orders = candidate_orders.filter(Order.price.isnot(None))

        # Make sure we order by best price for this direction
        if order.direction == DirectionType.ask.value:
            candidate_orders = candidate_orders.filter(Order.volume >= order.volume).order_by(Order.price.desc())
        else:
            candidate_orders = candidate_orders.filter(Order.volume <= order.volume).order_by(Order.price)

        reciprocal_order = candidate_orders.first()
        if reciprocal_order is not None:
            return execute(session, order, reciprocal_order)
        else:
            logger.info('Cancelling order {} because result set is empty'.format(order.id))
            order.cancel(session)

    elif order.order_type == OrderType.limit_order.value:
        # if len(candidate_orders.filter(Order.price.is_(None))): pass
        if order.direction == DirectionType.ask.value:
            candidate_orders = candidate_orders.filter(Order.price >= order.price).order_by(Order.price.desc())
        else:
            candidate_orders = candidate_orders.filter(Order.price <= order.price).order_by(Order.price)

        reciprocal_order = candidate_orders.first()
        if reciprocal_order is not None:
            return execute(session, order, reciprocal_order)

        more_candidates = session.query(Order)\
            .filter(Order.user != order.user)\
            .filter(Order.direction == reciprocal_direction)\
            .filter(Order.state == OrderStateType.in_market.value)\
            .filter(Order.contract == order.contract)\

        if order.direction == DirectionType.ask.value:
            more_candidates = more_candidates\
                .filter(Order.price / Order.volume >= order.price_to_volume)\
                .order_by(Order.volume.desc())
        else:
            more_candidates = more_candidates\
                .filter(Order.price / Order.volume <= order.price_to_volume)\
                .order_by(Order.volume)

        reciprocal_order = more_candidates.first()
        if reciprocal_order is not None:
            return execute(session, order, reciprocal_order)
