# -*- coding: utf-8 -*-
""" Dataloading class for backtesting.
"""
from utils.common import *
from utils.timeseriessplit import TimeSeriesSplitCustom
from dataviewer import Viewer

class Loader:
    """ Dataloading class.
    """
    def __init__(self):
        log_path = '.'.join(['backtest', __name__])
        #self.logger = logging.getLogger(log_path)
        trade_dir = os.path.dirname(os.path.realpath(__file__))
        parent_dir = os.path.abspath(trade_dir + '/../')
        self.data_path = os.path.join(parent_dir, 'datamining/data/')
        self.price_path = os.path.join(self.data_path, 'exchanges/')
        self.viewer = Viewer()


    def get_prices(self, coin):
        """ Gets price data (OHLCV) for COIN.
        """
        return self.viewer.get_price_data(coin)

    def get_y(self, df, ternary=False):
        """ Generates Y DataFrame for training/test set.
            This method generates labels when the Y is ternary:
            Y {-1, 1}.
        """
        
        return self.get_avg(df, col='close')

    def get_avg(self, df, col=''):
        """ Generates average for COL for DF. COL is expected to have
            multiple values for multiple exchanges, ex. multiple Open
            prices. Will only consider USD for now. May consider Volume
            weighing in the future.
            Args:
                df (pd.DataFrame): DataFrame to average COLs.
                col (str): Columns to average for each row.
            Returns:
                avg_df (pd.DataFrame): DataFrame with averaged values with
                    axis=1.
        """
        avg_cols = [x for x in df.columns if 'USD' in x and col in x]
        avg_df = df[avg_cols].mean(axis=1)
        return avg_df

    def split_dataset(self, df, split_all=True, split_size=24, train_splits=24):
        """ Splits DF into training, and test sets.
            Cross-validation will be done in a walk-forward fashion. 
            Args:
                df (pd.DataFrame): DataFrame to split
                split_size (int): # of ticks to distribute to each split.
                split_all (bool): Determines whether to make each tick a split.
                    If False, each split will have 1+ ticks.
            Returns:
                split (iterable): df split into training, cv
                test_i (np.array): array of range with test indices
        """
        n_samples = len(df)
        n_test = n_samples // 4
        test_start = n_samples - n_test + 1
        test_i = np.arange(test_start, n_samples - 1)

        if split_all:
            n_splits = n_samples - n_test
        else:
            n_splits = (n_samples - n_test) // split_size - 1
        tscv = TimeSeriesSplitCustom(n_splits=n_splits)
        split = tscv.split(df[:test_start], fixed_length=True,
                train_splits=train_splits,
                test_splits=1)
        return split, test_i 

    def preprocess(self, df, stationarity=False, lag=False, n_lags=5):
        if stationarity:
            df = self.difference(df)
        if lag:
            df = self.lag(df, n_lags=n_lags)

        return df

    def difference(self, df):
        """ Returns differentiated price data to insure stationarity
        """
        diff_df = df.diff(periods=1)
        diff_df = diff_df.dropna()
        return diff_df


    def invert_diff(self, diff_df, price_df):
        """ Inverts difference to create price DataFrame.
        """
        return price_df + diff_df

    def lag(self, df, n_lags):
        """ Adds lagged price data to DataFrame.
            Args:
                df (pd.DataFrame): DataFrame with price data.
                n_lags (int): Number of lags to input as features into DF.
            Returns:
                df (pd.DataFrame): DataFrame with lags as features
        """
        return_df = df
        for i in range(1, n_lags+1):
            lag_df = df.shift(i)
            return_df = return_df.merge(lag_df, left_index=True,
                    right_index=True,
                    suffixes=('', '_lag{}'.format(i)))
        return_df.dropna(axis=0, inplace=True)
        return return_df
