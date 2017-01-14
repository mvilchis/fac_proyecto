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
    by the data and searchs for its coordinates.
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

## is_numeric
def is_numeric(data, col_id):
    '''
    This function identifies if the given column
    is of type numeric.
    '''
    if not  np.issubdtype(df_invoices[col_id].dtype, np.number): raise AssertionError
    if pd.isnull(df_invoices[col_id]).all(): raise  AssertionError

## is_null
def is_null(data, col_id):
    '''
    This function checks if the given column
    contains null types.
    '''
    if pd.isnull(df_invoices[col_id]).all(): raise  AssertionError
    if not  np.issubdtype(df_invoices[col_id].dtype, np.number): raise AssertionError

## is_valid
def is_valid(data, col_id, valid_values, thresh = .01):
    '''
    This function checks if the valid values
    are below a given threshold, marks an error in such case,
    and returns the dataset containing only valid values.
    '''
    valids = data[not data[col_id].isin(valid_values)]
    if len(valids) >= len(data) * thresh: raise AssertionError
    return data[data[col_id].isin(valid_values)]

## rem_nulls
def rem_nulls(data, col_id):
    '''
    This function removes null values.
    '''
    return data[col_id].apply(lambda x: 0 if pd.isnull(x) else x)

##    data = pd.read_csv('../data/export_V_TaxEntities.csv')
