# -*- coding: utf-8 -*-
""" Test Strategy class.
"""
from common import *
from strategy import Strategy
from dataloader import Loader

class TestStrategy(Strategy):
    """ Test Strategy. Uses Random Walk model from BasicModel to predict
        price action.
    """
    def next(self):
        if self.order:
            return

        mean, std = self.params.model.model
        random_walk = np.random.normal(mean, std)
        forecast = self.dataclose[0] * (1 + random_walk)
        if not self.position:
            if forecast > self.dataclose[0]:
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            if  (forecast < self.dataclose[0]): #and \
                    #(forecast > self.buyprice):
            #if self.dataclose[0] > self.buyprice:
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                self.order = self.sell()


    def train(self):
        self.params.model.train()
