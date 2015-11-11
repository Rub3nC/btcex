import logging
from datetime import datetime

from sqlalchemy import Column, Integer, ForeignKey, DateTime, Enum, Numeric, Boolean
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql import exists, and_

from models import Base
from models.asset import Asset
from models.account import Holding
from models.order import Order
from models.consts import OrderStateType


logger = logging.getLogger(__file__)


class Contract(Base):

    """This is essentially an abstract base class"""

    __tablename__ = 'contracts'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    contract_type = Column(Enum('Commodity', 'Future', name='contract_types'), nullable=False)
    issuer_id = Column(Integer, ForeignKey('users.id'), nullable=False)

    issuer = relationship('User', backref=backref('contracts'))

    __mapper_args__ = {'polymorphic_on': contract_type}


class FuturesContract(Contract):

    __tablename__ = 'futures'
    __mapper_args__ = {'polymorphic_identity': 'Future'}

    id = Column(Integer, ForeignKey('contracts.id'), primary_key=True)

    # If there are orders that refer to this contract, we set `cancelled` to `True`
    cancelled = Column(Boolean, default=False, nullable=False)

    # Indicates whether `expire()` method has been run already
    expired = Column(Boolean, default=False, nullable=False)

    # Upon expiration of this contract, the holders of this contract will receive a percentage of this volume
    expires_at = Column(DateTime, nullable=False)
    volume = Column(Numeric(precision=10, scale=4))
    asset_id = Column(Integer, ForeignKey('assets.id'))

    # This futures contract is itself also an asset, of which you can hold a certain volume
    contract_asset_id = Column(Integer, ForeignKey('assets.id'))

    asset = relationship('Asset', foreign_keys=[asset_id])
    contract_asset = relationship('Asset', foreign_keys=[contract_asset_id])

    @classmethod
    def create_contract(cls, session, user, expires_at, asset, asset_volume, contract_asset_name, contract_volume):
        if user.volume_of_asset(session, asset) < asset_volume:
            return None, None

        contract_asset = Asset.create_asset(contract_asset_name)
        contract = cls(created_at=datetime.now(), contract_type='Future', issuer=user, expires_at=expires_at,
                       volume=asset_volume, asset=asset, contract_asset=contract_asset)
        session.add_all([contract_asset, contract])
        user.increase_volume_of_asset(session, contract_asset, contract_volume)
        user.decrease_volume_of_asset(session, asset, asset_volume)

        logger.info('Created futures contract instance and asset with name "{}"'.format(contract_asset_name))

        return contract, contract_asset

    def can_be_used_in_order(self):
        return not self.cancelled and not self.expired and datetime.now() <= self.expires_at

    def cancel(self, session):
        users_and_holdings = Holding.users_that_hold_asset(session, self.contract_asset)
        if any(user for user, volume_sum in users_and_holdings if user is not self.issuer):
            logger.info('Cannot cancel futures contract {} if other people hold it'.format(self.id))
            return False

        valid_states = (OrderStateType.cancelled.value, OrderStateType.executed.value)
        if session.query(exists().where(and_(Order.contract == self, Order.state.notin_(valid_states)))).scalar():
            logger.info('There are orders not in state (cancelled, executed) for contract {}'.format(self.id))
            return False

        if self.expired or self.expires_at < datetime.now():
            logger.info('The expire() method has been run or exp. date has passed for contract {}'.format(self.id))
            return False

        if self.cancelled:
            logger.info('The contract {} has already been cancelled'.format(self.id))
            return False

        # Return funds that was taken for deposit
        self.issuer.increase_volume_of_asset(session, self.asset, self.volume)

        # Remove entire volume held in the future
        self.issuer.decrease_volume_of_asset(session, self.contract_asset,
                                             self.issuer.volume_of_asset(session, self.contract_asset))

        # We cannot delete the asset from database since we know that at least one `Holding` refers to it
        self.contract_asset.remove(session)

        if not session.query(Order).count():
            session.delete(self)
        else:
            self.cancelled = True
            session.add(self)

        session.commit()
        logger.info('Cancelled contract {}'.format(self.id))
        return True

    def expire(self, session):
        if self.expired:
            return

        users_and_holdings = Holding.users_that_hold_asset(session, self.contract_asset)

        # We may assume that all holdings are strictly positive
        total_volume = sum(volume_sum for _, volume_sum in users_and_holdings)
        for user, volume_sum in users_and_holdings:
            user.increase_volume_of_asset(session, self.asset, volume_sum / total_volume * self.volume)
            logger.info('Distributed {} of asset {} to user {}'.format(volume_sum, self.asset_id, user.id))

        self.expired = True
        session.add(self)
        session.commit()
