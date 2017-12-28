""" Exchange module. Will write each exchange's trading
    pairs to the dataset, as well as the coin price data
    for each pair.
"""

class Exchange():
    def __init__(self):
        self.exchanges = None
        self.df = None

    def make_list(self):
        """ Creates list of all exchanges.
        """


