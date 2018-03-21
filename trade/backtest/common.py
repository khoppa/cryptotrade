# -*- coding: utf-8 -*-
""" Common imports for modules in backtest. Also imports modules from its parent
    class, /trade
"""
import os
import sys
backtest_dir = os.path.dirname(os.path.realpath(__file__))
trade_dir = os.path.abspath(backtest_dir + '/../')
sys.path.append(trade_dir)
from utils.common import *
from utils.logger import *
from dataviewer import Viewer
from dataloader import * 

from sklearn.metrics import mean_absolute_error
