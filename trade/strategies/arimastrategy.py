# -*- coding: utf-8 -*-
""" ARIMA Strategy class.
"""
from common import *
from strategy import Strategy
from dataloader import Loader
from datetime import datetime

class ArimaStrategy(Strategy):
    """ ARIMA Strategy. Uses ARIMA model from Arima 
        to predict price action.
    """
    def next(self):
        if self.order:
            return

        forecast = self.model.predict(self.datas[0].datetime.datetime())
        if not self.position:
            if forecast > 0:
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            #if  (forecast < 0): #or \
            #        #(forecast + self.dataclose[0] < self.buyprice):
            if self.dataclose[0] > self.buyprice:
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                self.order = self.sell()
