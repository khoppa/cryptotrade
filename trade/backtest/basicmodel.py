# -*- coding: utf-8 -*-
""" Basic Model class.
"""
from common import *
from model import Model

class BasicModel(Model):
    """ Basic model. Uses previous price as prediction for next price,
        with deviations based on mean, standard deviation of differences.
        Aka Random Walk.
    """
    def train(self):
        close_avg = self.df['close']
        y_pred = close_avg.shift(1)

        diff_df = close_avg.diff(periods=1)
        diff_df = diff_df.dropna()


        diff_df = diff_df.values / close_avg[1:]
        mean = np.mean(diff_df)
        std = np.std(diff_df)

        np.random.seed(44)
        random_walk = np.random.normal(mean, std, len(y_pred))
        y_pred = y_pred * (1+random_walk)

        y_pred.dropna(inplace=True)
        print self.error(y_pred, close_avg[1:])
        #self.visualize(pd.to_datetime(self.df['timestamp'][1:], unit='s'),
        #        y_pred, close_avg[1:])
        self.model = [mean, std]
