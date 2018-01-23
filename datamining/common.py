# -*- coding: utf-8 -*-
""" This file contains common modules and packages that will be imported into
    the modules for the datamining of the coin market price data.
"""

# OS/Standard imports
import sys
import os
import io
from os import listdir
import time
import logging
import math
import re
import string
import json
import requests

# Unicode sorting
#import icu

# Numerical Packages
import numpy as np

import matplotlib.pyplot as plt

# Data Loading Packages
import cv2
import tables
import pickle

# Machine Learning Packages
import pandas as pd

# Progress Bar
from tqdm import tqdm
from tqdm import trange
