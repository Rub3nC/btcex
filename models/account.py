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
        current_holdings = Holding.current_holdings(session, self)
        return current_holdings[asset.id]

    def get_holdings(self, session, asset):
        current_holdings = Holding.current_holdings(session, self)
        return current_holdings[asset.id]

    def increase_volume_of_asset(self, session, asset, volume):
        holding = Holding.create_holding(session, self, asset, volume)
        session.add(holding)
        return holding

    def decrease_volume_of_asset(self, session, asset, volume):
        holding = Holding.create_holding(session, self, asset, -volume)
        session.add(holding)
        return holding


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
        holding = cls(user=user, asset=asset, volume=volume, source=source, description=description)
        session.add(holding)
        return holding

    @classmethod
    def current_holdings(cls, session, user):
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
            if volume_sum > 0:
                ret.append((session.query(User).get(user_id), volume_sum))

        return ret