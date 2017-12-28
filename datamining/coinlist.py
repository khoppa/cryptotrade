""" Module for Coinlist class, which holds information of all
    traded pairs for an Exchange.
"""

class Coinlist():
    def __init__(self, exchange):
        self.pairs = None
        self.exchange = exchange

    def template(self):
        """ Creates template coinlist which will include all
            coins as rows, and any possible trading pair as
            columns. Dummy variables will indicate whether
            the row-column trading pair exists on the exchange.
        """

