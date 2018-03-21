#from utils.common import *
from backtest.basicmodel import BasicModel
from backtest.arima import Arima
from backtest.randomforest import RandomForest
from backtest.common import *
from trader import Trader

arr = np.arange(240)
df = pd.DataFrame(arr, columns=['Price'])
"""
split, test = loader.split_dataset(df)
for train, cv in split:
    print 'train:'
    print len(train)
    print 'cv:'
    print len(cv)
print 'test:'
print len(test)
"""
#print loader.lag(df, 5).head()
#model = BasicModel('BTC')
#model = Arima('BTC')
#model = RandomForest('BTC')
#model.train()
#model.load_data()
#model.test()
#model.predict()
#loader = Loader('BTC')
#viewer = Viewer()

trader = Trader()
trader.trade()
