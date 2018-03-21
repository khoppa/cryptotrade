# -*- coding: utf-8 -*-
""" Strategy class.
"""
from common import *

class Strategy(bt.Strategy):
    """ Strategy class for backtrader. Allows a model to be associated with it.
        Models can be trained outside of backtrader so the trained model
        is used to create indicators for trading.
    """
    params = (
            ('loader', None),
            ('model', None),
            ('coin', None),
            ('df', None),
            ('split', None),
            ('backtest', False),
            )

    def __init__(self):
        self.model = self.params.model
        self.params.df, self.params.split = self.model.load_data(
                self.params.loader, self.params.df, self.params.split)
        if self.params.backtest:
            self.model.train()
        self.dataclose = self.datas[0].close

        self.order = None
        self.buyprice = None
        self.buycomm = None

    def log(self, txt, dt=None):
        """ Logging function for strategy.
        """
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def add_df(self):
        """ Adds training dataframe and training/cv splits for self.model.
            Args:
                df (pd.DataFrame): DataFrame with training data
                split (iter): Iterable which splits df into training and cv
                    sets for a timeseries.
        """
        self.params.model.load_data(df, split)
