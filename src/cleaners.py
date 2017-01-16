#! /bin/python

'''
------------------------------
# Authors:
  - Miguel Vilchis
  - Luis Roman
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
from yahoo_finance import Share

## ------------------------------
## Constants
## ------------------------------
MX_CURRENCY = 'MXN'
USD_CURRENCY = 'DLL'
EURO = 'EUR'
mx_currency = ['$','MN','MEX', 'MNX', 'NACIONAL', 'PESOS', 'NAL', 'TRANSFERENCIA', 'MXP']
usd_currency = ['DOLAR', 'DOLARES', 'DOLARES AMERICANOS','USD','DLL', 'DOL','US', 'US DOLLAR']
euro_currency = ['EUR', 'EURO', 'EUROS']
other_currencies = ['BDT', 'BHD', 'BOB' , 'CAD', 'COP', 'CRC', 'CZK','DZD', 'EEK', 'GBP', 'GHS', 'ISK',
                    'JPY', 'NGN', 'NOK', 'PAB', 'SVC', 'THB', 'TZS', 'VEF']



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

## from_dollar_to_mx
def from_dollar_to_mx(value, date):
    '''
    This function convert from dollar to mxn
    depends on date
    '''
    currency = Share('MXN=x')
    historical = currency.get_historical(date, date)
    return value*float(historical[0]['High'])

## search_currency
def search_currency(clean_currency, date):
    '''
    This function search value of currency by date
    '''
    currency = Share(clean_currency+'=x')
    historical = currency.get_historical(date, date)
    return from_dollar_to_mx(float(historical[0]['High']), date)

## get_distance_one
def get_distance_one (row_item):
    '''
    This function obtain all edits that are one edit
     away from row_item
    '''
    alphabet   = string.uppercase
    splits     = [(row_item[:i], row_item[i:]) for i in range(len(row_item) +1)]
    deletes    = [a + b[1:] for a, b in splits if b]
    transposes = [a + b[1] + b[0] + b[2:] for a,b in splits if len(b) > 1]
    replaces   = [a + c+ b[1:] for a,b in splits for c in alphabet if b]
    inserts    = [a + c+ b     for a,b in splits for c in alphabet]
    return set(deletes + transposes + replaces + inserts)


## search_in_list
def search_in_list (list_words, candidates):
    '''
    This function return intersection of lists
    '''
    return set( w for w in candidates if w in list_words)

## normalize_currency
def normalize_currency(raw_currency):
    '''
    This function return standard currency name
    '''
    raw_currency = raw_currency.upper().strip()
    raw_currency = raw_currency.replace(".","")
    # First search directly
    if raw_currency in mx_currency:
        return MX_CURRENCY
    if raw_currency in usd_currency:
        return USD_CURRENCY
    if raw_currency in euro_currency:
        return EURO
    if raw_currency in other_currencies:
        return raw_currency

    #Search with modifications

    raw_currency_list = raw_currency.split(' ')
    candidates = set()
    for raw_currency in raw_currency_list:
        candidates.update(get_distance_one(raw_currency))
    mx_search    = search_in_list(mx_currency, candidates)
    usd_search   = search_in_list(usd_currency, candidates)
    euro_search  = search_in_list(euro_currency, candidates)
    other_search = search_in_list(other_currencies, candidates)

    #Return most probable
    mx_len = len(mx_search)
    usd_len = len(usd_search)
    euro_len = len(euro_search)
    other_len = len(other_search)
    max_value = max (mx_len, usd_len, euro_len, other_len)
    if max_value == 0:
        return 'Error'
    if max_value == mx_len:
        return MX_CURRENCY
    if max_value == usd_len:
         return USD_CURRENCY
    if max_value == euro_len:
        return EURO
    if max_value == other_search:
        return other_search.pop()
    return 'Error'

##    data = pd.read_csv('../data/export_V_TaxEntities.csv')


