from flask import Flask

from models import Session


app = Flask('btcex')
app.config['SECRET_KEY'] = 'secret key here'
session = Session()
