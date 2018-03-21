# -*- coding: utf-8 -*-
""" Arima model implementation.
"""
from common import *
from model import Model
from statsmodels.tsa.arima_model import ARIMA

class Arima(Model):
    """ Arima model.
    """
    def load_data(self, loader, df, split):
        self.loader = loader
        self.df = df
        self.split = split

        close_avg = pd.DataFrame({'close':self.df['close'].values},
                index=self.df.index)
        diff_df = self.loader.preprocess(close_avg, stationarity=True)
        diff_df.rename(columns={'close':'diff'}, inplace=True)
        
        self.df = close_avg.merge(diff_df, left_index=True, right_index=True)
        return self.df, self.split


    def train(self):
        """
        predict = []
        true = []
        for train, cv in self.split:
            try:
                self.model = ARIMA(self.df.iloc[train]['diff'], order=(1,1,0))
                model_fit = self.model.fit(disp=False)
                predict.append(model_fit.forecast()[0][0])
                true.append(self.df.iloc[cv]['diff'])
            except:
                continue

        df = self.df['close'][24:24+len(predict)]
        price_df = self.loader.invert_diff(predict, df)
        price_df.dropna(inplace=True)
        error = self.error(price_df, df)
        print 'MAE: %f' % error
        self.visualize(self.df.index.values[24:24+len(predict)], price_df, df)
        """

    def predict(self, datetime):
        idx = self.df.index.get_loc(datetime)
        forecast = 0
        try: 
            self.model = ARIMA(self.df.iloc[idx-12:idx]['diff'], order=(1,1,0))
            model_fit = self.model.fit(disp=False)
            forecast = model_fit.forecast()[0][0]
        except:
            pass 
        self.forecasts.append(forecast)
        return forecast
