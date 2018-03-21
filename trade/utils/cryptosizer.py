# -*- coding: utf-8 -*-
""" Custom Sizer for Cryptocurrencies, allowing for fractional orders.
"""

from common import *
import backtrader as bt
from decimal import Decimal, ROUND_DOWN

class CryptoSizer(bt.Sizer):
    """ Custom Crypto Sizer.
    """
    params = (
            ('stake', 0.1),
            )

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            size = Decimal(cash / data.close[0])
            size = Decimal(size.quantize(Decimal('0.01'), rounding=ROUND_DOWN))
            return float(size)
        position = self.broker.getposition(data)
        if not position.size:
            return 0
        else:
            return position.size
        

