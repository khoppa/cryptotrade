from common import *
from scrape import Scraper
from dataminer import Miner

url = 'https://min-api.cryptocompare.com/data/all/exchanges'
scraper = Scraper()
df = scraper.get_tradepair_data(url)

miner = Miner()
"""
major_exchanges = ["BitTrex", "Bitfinex", "Bithumb", "Bitstamp", \
        "Coinbase", "Gemini", "bitFlyer", "Binance", "Coinone", \
        "HitBTC", "Poloniex", "Kraken", "Korbit", "Huobi"]

"""
major_exchanges = ['Binance']
#coins = miner.get_exchange_coins(major_exchanges)
#miner.make_dataset('Binance', 'BTM')
#miner.import_supply()
coinlist = []
for f in listdir('data/exchanges'):
    if f.endswith('.csv'):
        coinlist.append(f.split(".")[0])
idx = coinlist.index('UET')
print coinlist[idx:]
