# -*- coding: utf-8 -*-
""" Trader class.
"""
from utils.common import *
from utils.cryptosizer import CryptoSizer
from dataloader import Loader
import backtrader as bt

class Trader(object):
    """ Trader class responsible for using backtrader to carry out trades.
        When instantiated, will load the data which will be fed into models.
        Uses bt.Strategy's which may use indicators generated from Models
        to trade.
    """
    def __init__(self):
        log_path = '.'.join(['trade', __name__])
        self.logger = logging.getLogger(log_path)
        trade_dir = os.path.dirname(os.path.realpath(__file__))
        parent_dir = os.path.abspath(trade_dir + '/../')
        self.data_path = os.path.join(parent_dir, 'datamining/data/')
        self.price_path = os.path.join(self.data_path, 'exchanges/')
        self.loader = Loader()
        self.cerebro = bt.Cerebro()

    def trade(self, strategy, params, coin, backtest=True):
        """ Initiates trading via backtrader.
        """
        df = self.import_data(coin)
        #data = bt.feeds.PandasData(dataname=df)
        split, test_i = self.loader.split_dataset(df)
        if backtest:
            self.cerebro.addstrategy(strategy, loader=self.loader,
                    model=params[0], coin=params[1],
                    df=df, split=split,
                    backtest=backtest)

        data = bt.feeds.PandasData(dataname=df.iloc[test_i])
        self.cerebro.broker.setcommission(0.001)
        self.cerebro.adddata(data)
        self.cerebro.broker.setcash(10000.0)
        self.cerebro.addsizer(CryptoSizer)
        self.cerebro.run()
        self.cerebro.plot()
        print self.cerebro.broker.getvalue()

    def import_data(self, coin):
        """ Imports COIN price data.
            Args:
                coin (str): coin to add data for.
            Returns:
                df (pd.DataFrame): DataFrame with price data.
        """
        df = self.loader.get_prices(coin)
        close_avg = self.loader.get_y(df, ternary=False)
        close_avg = pd.DataFrame({'close':close_avg.values},
                index=df.index)
        for feat in ['open', 'high', 'low', 'volumeto', 'volumefrom']:
            temp = self.loader.get_avg(df.loc[close_avg.index], col=feat)
            close_avg[feat] = temp.values
        close_avg.rename(columns={'volumeto':'volume'}, inplace=True)
        close_avg.drop(columns=['volumefrom'], inplace=True)
        return close_avg.dropna(axis=0)

    def add_strategy(self, strategy):
        self.cerebo.addstrategy(strategy)
