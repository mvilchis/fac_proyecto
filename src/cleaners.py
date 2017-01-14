#! /bin/python

'''
------------------------------
# Authors:
  - Miguel Vilchis
  - Luis Román
------------------------------
# Description:
This script contains multiple utileries
for cleaning and processing datasets.
------------------------------
# Usage:


'''

## ------------------------------
## Libraries
## ------------------------------
import csv
import pandas as pd
import numpy as np
from dateutil import parser
import datetime
import string
import json
import urllib
## ------------------------------
## Functions
## ------------------------------

## rem_non_ascii
def rem_non_ascii(sentence):
    '''
    This function removes all non-ascii
    characters in sentence and sets every
    word to lower case.
    '''
    printable = set(string.printable)
    return filter(lambda x: x in printable, sentence).lower()


## get_coords
def get_coords(data, addres_cols):
    '''
    This function constructs the address provided
    by the dataset and searchs for its coordinates.
    '''
    base_url  = "https://maps.googleapis.com/maps/api/geocode/json?address="
    address   = (data.City.replace(np.nan, '', regex = True).replace(" +", "+", regex = True) + "," +
                 data.Municipality.replace(np.nan, '', regex = True).replace(" +", "+", regex = True) + "," +
                 data.ZipCode.replace(np.nan, '', regex = True).replace(" +", "+", regex = True))
    address   = address.apply(rem_non_ascii)
    full_url  = base_url + address
    res       = full_url.apply(urllib.urlopen)
    data      = res.apply(lambda x: json.loads(x.read()))
    return data
