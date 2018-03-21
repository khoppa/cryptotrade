# -*- coding: utf-8 -*-
""" Random Forest Strategy class.
"""
from common import *
from strategy import Strategy
from dataloader import Loader
from datetime import datetime

class RandomForestStrategy(Strategy):
    """ Random Forest Strategy. Uses Random Forest model from RandomForest
        to predict price action.
    """
    def next(self):
        if self.order:
            return

        forecast = self.model.predict(self.datas[0].datetime.datetime())
        if not self.position:
            if forecast > self.dataclose[0]:
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            if  (forecast < self.dataclose[0]): #and \
                    #(forecast > self.buyprice):
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                self.order = self.sell()
