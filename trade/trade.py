# -*- coding: utf-8 -*-
""" Trading script
"""
from utils.common import *
from trader import Trader
from strategies.teststrategy import TestStrategy
from strategies.rfstrategy import RandomForestStrategy
from strategies.arimastrategy import ArimaStrategy
from backtest.basicmodel import BasicModel
from backtest.randomforest import RandomForest
from backtest.arima import Arima
import backtrader as bt

if __name__ == '__main__':
    trader = Trader()
    coin = 'BTC'
    #model = BasicModel(coin) 
    #strategy = TestStrategy
    #model = RandomForest(coin)
    #strategy = RandomForestStrategy
    #model = Arima(coin)
    #strategy = ArimaStrategy
    params = [model, coin]
    trader.trade(strategy, params, coin, backtest=True)
    print model.forecasts
