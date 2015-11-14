import os
import unittest
from decimal import Decimal
from datetime import datetime, timedelta

from sqlalchemy import inspect
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base
from models.account import User
from models.asset import Asset
from models.order import Order, Transaction
from models.contract import FuturesContract
from models.consts import OrderType
from market.market import put_order


# We use Postgres for testing since SQLite doesn't have an INTERVAL data type
engine = create_engine('postgres://btcex:{}@localhost:5432/btcex_test'.format(os.environ.get('BTCEX_TEST_PW')))
Session = sessionmaker(bind=engine)


class FuturesTest(unittest.TestCase):
    def setUp(self):
        self.session = Session()
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        self.now = datetime.now()

    def tearDown(self):
        self.session.commit()
        Base.metadata.drop_all(bind=engine)

    def test_normal_scenario_with_two_users(self):
        # First create some users
        user1 = User.create_user(self.session, 'user1', 'abcd')
        user2 = User.create_user(self.session, 'user2', 'abcd')

        # Make sure user1 has 1 BTC for later deposit into a futures contract
        btc = Asset.create_asset('BTC')
        user1.increase_volume_of_asset(self.session, btc, Decimal('1'))

        # Create a futures contract worth 1 BTC in +14 days
        contract, asset = FuturesContract.create_contract(self.session, user1, datetime.now() + timedelta(days=14), btc,
                                                          Decimal('1'), 'FUTURE', Decimal('100'))
        # This will fail if user1 has insufficient funds (i.e. < 1 BTC)
        assert contract is not None
        assert asset is not None
        self.session.commit()

        # Create an order for this contract (i.e. newly created asset)
        usd = Asset.create_asset('USD')
        ask_order = Order.create_order(self.session, user1, Decimal('20'), usd, contract, Decimal('50'), False,
                                       OrderType.limit_order.value)
        # Make sure we have enough funds
        assert ask_order is not None
        self.session.commit()

        # Put order into market
        assert put_order(self.session, ask_order) is None

        # Create a bid order from user2
        user2.increase_volume_of_asset(self.session, usd, Decimal('20'))
        bid_order1 = Order.create_order(self.session, user2, Decimal('20'), usd, contract, Decimal('50'), True,
                                        OrderType.limit_order.value)
        # Make sure we have enough funds
        assert bid_order1 is not None

        transaction = put_order(self.session, bid_order1)
        assert isinstance(transaction, Transaction)
        assert transaction.ask_order is ask_order
        assert transaction.bid_order is bid_order1

        contract.expire(self.session)
        assert user1.volume_of_asset(self.session, btc) == Decimal('0.5')
        assert user2.volume_of_asset(self.session, btc) == Decimal('0.5')

        # Run it again. Must be idempotent.
        contract.expire(self.session)
        assert user1.volume_of_asset(self.session, btc) == Decimal('0.5')
        assert user2.volume_of_asset(self.session, btc) == Decimal('0.5')

    def test_insufficient_funds(self):
        # Create a user and asset
        user = User.create_user(self.session, 'user', 'abcd')
        usd = Asset.create_asset('USD')
        self.session.add_all([user, usd])
        self.session.commit()

        # This user creates a contract without proper funding
        contract, asset = FuturesContract.create_contract(self.session, user, datetime.now() + timedelta(days=14), usd,
                                                          Decimal('1'), 'FUTURE', Decimal('100'))
        # Assert that this does not work
        assert contract is None
        assert asset is None

    def test_cancel_contract(self):
        # Create a user and asset
        user = User.create_user(self.session, 'user', 'abcd')
        usd = Asset.create_asset('USD')
        self.session.add_all([user, usd])
        self.session.commit()

        # Add funds to user1 so that we can create a contract
        user.increase_volume_of_asset(self.session, usd, Decimal('1'))

        # Let this user create a contract
        contract, asset = FuturesContract.create_contract(self.session, user, datetime.now() + timedelta(days=14), usd,
                                                          Decimal('1'), 'FUTURE', Decimal('100'))
        assert contract is not None
        assert asset is not None

        # Now, money have been withdrawn from user1's account
        assert user.volume_of_asset(self.session, usd) == Decimal('0')

        # Cancel the contract
        assert contract.cancel(self.session) is True
        assert inspect(contract).deleted is True
        assert user.volume_of_asset(self.session, usd) == Decimal('1')

        # OK, the contract is cancelled.
        # Now do the same thing, but this time, create an order and verify that the cancelled flag is set instead
        contract, asset = FuturesContract.create_contract(self.session, user, datetime.now() + timedelta(days=14), usd,
                                                          Decimal('1'), 'FUTURE', Decimal('100'))
        assert contract is not None
        assert asset is not None
        assert user.volume_of_asset(self.session, usd) == Decimal('0')

        ask_order = Order.create_order(self.session, user, Decimal('20'), usd, contract, Decimal('50'), False,
                                       OrderType.limit_order.value)
        assert ask_order is not None
        assert user.volume_of_asset(self.session, asset) == Decimal('50')

        # Assert that we cannot cancel the contract if there are created orders
        assert contract.cancel(self.session) is False

        # Put order into market
        assert put_order(self.session, ask_order) is None

        # Assert that we cannot cancel the contract if there are orders in the market
        assert contract.cancel(self.session) is False

        # It should be possible to cancel this order
        assert ask_order.cancel(self.session) is True
        assert user.volume_of_asset(self.session, asset) == Decimal('100')

        # Check that order is in the expected state
        assert contract.cancel(self.session) is True
        assert inspect(contract).deleted is False
        assert contract.cancelled is True
