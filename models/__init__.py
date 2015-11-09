import os

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker


Base = declarative_base()
engine = create_engine('postgres://btcex:{}@localhost/btcex'.format(os.environ.get('BTCEX_PASSWORD')))
Session = sessionmaker(bind=engine)
