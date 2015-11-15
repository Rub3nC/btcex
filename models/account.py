import logging
from decimal import Decimal
from collections import defaultdict

from sqlalchemy import Column, Integer, String, ForeignKey, Numeric, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from models import Base


logger = logging.getLogger(__file__)


class User(Base):

    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True)
    password_hash = Column(String)

    @classmethod
    def create_user(cls, session, username, password):
        user = cls(username=username, password_hash=password)
        session.add(user)
        return user

    def volume_of_asset(self, session, asset):
        current_holdings = Holding.current_holdings_for_user(session, self)
        return current_holdings[asset.id]

    def increase_volume_of_asset(self, session, asset, volume):
        holding = Holding.create_holding(session, self, asset, volume)
        if holding is not None:
            session.add(holding)
            logger.info('Increased holding of asset {} for user {} with {}'.format(asset.id, self.id, volume))
            return holding
        else:
            logger.warning('Could not increase holding of asset {} for user {} ({})'.format(asset.id, self.id, volume))
            return None

    def decrease_volume_of_asset(self, session, asset, volume):
        holding = Holding.create_holding(session, self, asset, -volume)
        if holding is not None:
            session.add(holding)
            logger.info('Decreased holding of asset {} for user {} with {}'.format(asset.id, self.id, volume))
            return holding
        else:
            logger.warning('Could not decrease holding of asset {} for user {} ({})'.format(asset.id, self.id, volume))
            return None


class Holding(Base):

    __tablename__ = 'holdings'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    asset_id = Column(Integer, ForeignKey('assets.id'))
    volume = Column(Numeric(precision=10, scale=4))
    source = Column(Enum('InternalTrade', 'External', name='source_types'))
    description = Column(String(250))

    user = relationship('User')
    asset = relationship('Asset')

    @classmethod
    def create_holding(cls, session, user, asset, volume, source='InternalTrade', description=None):
        logger.info('Trying to increase/decrease volume in asset {} for user {}'.format(asset.id, user.id))
        if asset.removed:
            logger.warning('Tried to increase/decrease volume in a removed asset ({})'.format(asset.id))
            return None

        if not volume:
            logger.warning('Tried to create a holding with zero volume; aborting')
            return None

        if volume < 0:
            holdings = cls.current_holdings_for_user(session, user)
            if holdings[asset.id] + volume < 0:
                logger.warning('Total vol. < 0 aborting. Ass. vol. {}, delta vol {}'.format(holdings[asset.id], volume))
                return None

        holding = cls(user=user, asset=asset, volume=volume, source=source, description=description)
        return holding

    @classmethod
    def current_holdings_for_user(cls, session, user):
        current = session.query(Holding.asset_id, func.sum(Holding.volume).label('volume_sum'))\
            .filter(Holding.user == user).group_by(Holding.asset_id)

        d = defaultdict(Decimal)
        for asset_id, volume_sum in current:
            d[asset_id] = volume_sum

        return d

    @classmethod
    def users_that_hold_asset(cls, session, asset):
        all_holdings = session.query(Holding.user_id, func.sum(Holding.volume).label('volume_sum'))\
            .filter(Holding.asset == asset).group_by(Holding.user_id)

        ret = []
        for user_id, volume_sum in all_holdings:
            ret.append((session.query(User).get(user_id), volume_sum))

        return ret
