# -*- coding: utf-8 -*-
""" Random Forest class.
"""
from common import *
from model import Model
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV

class RandomForest(Model):
    """ Random Forest Model.
    """
    def load_data(self, loader, df, split):
        self.loader = loader
        self.df = df
        self.split = split
        n_lags = 8

        close_avg = pd.DataFrame({'close':self.df['close'].values},
                index=self.df.index)
        lag_df = self.loader.preprocess(close_avg, lag=True, n_lags=n_lags)
        self.df = self.df.merge(lag_df, on='close', how='inner',
                left_index=True, right_index=True)
        
        for feat in ['open', 'high', 'low', 'volume']:
            temp = self.df[feat]
            temp = temp.shift(1)
            self.df[feat + '_avg_lag1'] = temp.values
            self.df.drop(feat, axis=1, inplace=True)
        self.df = self.df.dropna(axis=0)
 
        return self.df, self.split


        """ 
        close_avg = self.loader.get_y(self.df, ternary=False)
        close_avg = pd.DataFrame({'close_avg':close_avg.values},
                index=self.df.index)
        avg_df = self.loader.preprocess(close_avg, lag=True, n_lags=n_lags)
        for feat in ['open', 'high', 'low', 'volumeto', 'volumefrom']:
            temp = self.loader.get_avg(self.df.loc[avg_df.index], col=feat)
            temp = temp.shift(1)
            avg_df[feat + '_avg_lag1'] = temp.values
        self.df = avg_df.dropna(axis=0)
        split, test_i = self.loader.split_dataset(self.df)
        return split, test_i
        """

    def train(self):
        max_features = 'auto'
        n_estimators = 100
        min_samples_leaf = 1
        self.model = RandomForestRegressor(n_estimators=n_estimators,
                max_features=max_features, min_samples_leaf=min_samples_leaf)
        """
        params = {
                'n_estimators': [10, 500, 100, 500],
                'max_features': ['auto', 'sqrt', 'log2'],
                'min_samples_leaf': [1, 5, 10, 20]
                }
        clf = GridSearchCV(self.model, params, cv=self.split, verbose=0)
        clf.fit(self.df.drop('close_avg', axis=1), self.df['close_avg'])
        print clf.best_score_
        print clf.best_params_
        """

        predict = []
        true = []
        for train, cv in self.split:
            self.model.fit(self.df.iloc[train].drop('close', axis=1),
                    self.df.iloc[train]['close']) 
            predict.append(self.model.predict(self.df.iloc[cv].drop('close', axis=1)))
            true.append(self.df.iloc[cv]['close'])

        error = self.error(true, predict)
        print error

        """
        features = self.df.drop('close', axis=1).columns
        imp = rf.feature_importances_
        indices = np.argsort(imp)
        plt.bar(np.arange(len(indices)), imp[indices],
                tick_label=features[indices])
        plt.show()
        """
        
        #self.visualize(self.df.index.values[24:24+len(predict)], predict, true)

    def predict(self, datetime):
        """ Forecasts price for DATETIME. DATETIME is the current ticker, thus
            we must get the next DATETIME's features in self.df to predict the
            next ticker.
        """
        idx = self.df.index.get_loc(datetime)
        self.model.fit(self.df.iloc[idx-24:idx].drop('close', axis=1),
                self.df.iloc[idx-24:idx]['close'])
        return self.model.predict(self.df.iloc[idx+1].drop('close').reshape(1, -1))
