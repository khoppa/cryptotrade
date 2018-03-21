# -*- coding: utf-8 -*-
""" Model superclass for backtesting. Will be used as a base class for
    trading models.
"""
from common import *

class Model(object):
    """ Model superclass.
    """
    def __init__(self, coin):
        log_path = '.'.join(['backtest', __name__])
        self.logger = logging.getLogger(log_path)
        self.coin = coin
        self.loader = None
        self.df = None
        self.split = None
        self.model = None
        self.forecasts = []

    def load_data(self, loader, df, split):
        self.loader = loader
        self.df = df
        self.split = split
        return self.df, self.split

    def train(self):
        pass

    def predict(self):
        pass

    def error(self, pred, true):
        return mean_absolute_error(true, pred)

    def visualize(self, time, pred, true):
        """ Plots predicted and true values.
        """
        plt.plot(time, true, label='Actual')
        plt.plot(time, pred, label='Predicted')
        plt.xlabel('Time')
        plt.ylabel('Price ($)')
        plt.legend(bbox_to_anchor=(0.1, 1), loc=2, borderaxespad=0.,
                prop={'size': 14})
        plt.show()
