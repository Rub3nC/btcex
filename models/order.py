"""
A `market order` is an order that gets executed immediately, at the best possible price given a certain volume.
A `limit order` is an order with a specified price and volume.
"""

import logging
from datetime import datetime

from sqlalchemy import Column, Integer, Enum, DateTime, ForeignKey, Numeric, Interval, UniqueConstraint
from sqlalchemy.orm import relationship, backref

from models import Base
from models.account import User
from models.consts import DirectionType, OrderStateType


logger = logging.getLogger(__file__)


class Order(Base):

    __tablename__ = 'orders'

    # Boilerplate information
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    user_id = Column(Integer, ForeignKey('users.id'))

    # `price` is denoted in `asset`
    price = Column(Numeric(precision=15, scale=8), nullable=True)
    asset_id = Column(Integer, ForeignKey('assets.id'))

    # `volume` specifies how much of the `contract` this user wants to buy/sell
    volume = Column(Numeric(precision=10, scale=4))
    contract_id = Column(Integer, ForeignKey('contracts.id'))

    # Order specific information
    expires_in = Column(Interval, nullable=True)
    direction = Column(Enum('Bid', 'Ask', name='order_directions'))
    order_type = Column(Enum('MarketOrder', 'LimitOrder', name='order_types'))
    state = Column(Enum('Created', 'InMarket', 'Executed', 'Cancelled', name='order_states'))

    # If the order was matched with another order, this is when that happened
    executed_at = Column(DateTime)

    user = relationship(User, backref=backref('orders', order_by=id))
    asset = relationship('Asset')
    contract = relationship('Contract', backref=backref('orders', order_by=id.desc(), lazy='dynamic'))

    def __repr__(self):
        return "<Order {}>".format(self.id)

    @classmethod
    def create_order(cls, session, user, price, price_asset, contract, contract_volume, is_bid, order_type):
        if price_asset.removed or contract.contract_asset.removed:
            logger.error('Cannot create order with removed asset {}, {}'.format(price_asset.id,
                                                                                contract.contract_asset_id))
            return None

        if not contract.can_be_used_in_order():
            logger.error('Tried to use an inactive contract ({}) in an order'.format(contract.id))
            return None

        direction = DirectionType.bid.value if is_bid else DirectionType.ask.value
        order = cls(user=user, price=price, asset=price_asset, contract=contract, volume=contract_volume,
                    direction=direction, order_type=order_type, state='Created', created_at=datetime.now())

        if not is_bid and user.volume_of_asset(session, contract.contract_asset) >= contract_volume:
            user.decrease_volume_of_asset(session, contract.contract_asset, contract_volume)
        elif is_bid and user.volume_of_asset(session, price_asset) >= price:
            user.decrease_volume_of_asset(session, price_asset, price)
        else:
            logger.info('Insufficient funds for user {}'.format(user.id))
            return None

        session.add(order)
        return order

    def executed(self):
        return bool(self.executed_bid or self.executed_ask)

    def cancel(self, session):
        if self.state in (OrderStateType.created.value, OrderStateType.in_market.value):
            if self.direction == DirectionType.ask.value:
                asset = self.contract.contract_asset
                volume = self.volume
            else:
                asset = self.asset
                volume = self.price
            self.state = OrderStateType.cancelled.value
            self.user.increase_volume_of_asset(session, asset, volume)
            session.add(self)
            session.commit()
            return True
        else:
            logger.warning('Tried to cancel and refund order {}, but it is in state {}'.format(self.id, self.state))
            return False

    @property
    def price_to_volume(self):
        return self.price / self.volume


class Transaction(Base):

    """Basically an executed `Order` -- of course with corresponding buying/selling party"""

    __tablename__ = 'transactions'
    __table_args__ = (UniqueConstraint('ask_order_id', 'bid_order_id'),)

    id = Column(Integer, primary_key=True)

    # This is non-null when the method `execute_trade` has ben executed
    executed_at = Column(DateTime)

    # `ask_order`.contract, `bid_order`.contract and `contract` must be the same
    contract_id = Column(Integer, ForeignKey('contracts.id'), nullable=False)
    ask_order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    bid_order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)

    # Agreed upon `price`, which again is denoted in `asset`
    price = Column(Numeric(precision=15, scale=8))
    asset_id = Column(Integer, ForeignKey('assets.id'))

    # For the above `price`, `bid_order`.user gets `volume` amount of `contract` (the latter is also an asset)
    volume = Column(Numeric(precision=10, scale=4))

    contract = relationship('Contract', backref=backref('transactions', order_by=id.desc(), lazy='dynamic'))
    ask_order = relationship('Order', uselist=False, foreign_keys=[ask_order_id],
                             backref=backref('executed_ask', uselist=False))
    bid_order = relationship('Order', uselist=False, foreign_keys=[bid_order_id],
                             backref=backref('executed_bid', uselist=False))
    asset = relationship('Asset')

    def execute_trade(self, session):
        if self.executed_at is not None:
            return

        # Note that we have already *decreased* the volumes when we initially created the orders
        self.bid_order.user.increase_volume_of_asset(session, self.contract.contract_asset, self.volume)
        self.ask_order.user.increase_volume_of_asset(session, self.asset, self.price)
        self.executed_at = datetime.now()
        session.add(self)
        return True
