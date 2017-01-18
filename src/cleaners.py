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
from fixerio import Fixerio
import time
## ------------------------------
## Constants
## ------------------------------
MX_CURRENCY = 'MXN'
USD_CURRENCY = 'USD'
EURO = 'EUR'
CURRENCY_ERROR = 'Error'
mx_currency = ['$','MN','MEX', 'MNX', 'NACIONAL', 'PESOS', 'NAL', 'TRANSFERENCIA', 'MXP']
usd_currency = ['DOLAR', 'DOLARES', 'DOLARES AMERICANOS','USD','DLL', 'DOL','US', 'US DOLLAR']
euro_currency = ['EUR', 'EURO', 'EUROS']
other_currencies = [u'USD', u'IDR', u'BGN', u'ILS', u'GBP', u'DKK', u'CAD', u'HUF', u'RON', u'MYR',
                  u'SEK', u'SGD', u'HKD', u'AUD', u'CHF', u'KRW', u'CNY', u'TRY', u'HRK', u'NZD',
                  u'THB', u'EUR', u'NOK', u'RUB', u'INR', u'JPY', u'CZK', u'BRL', u'PLN', u'PHP',
                u'ZAR']
MISSING = 'Missing'


## ------------------------------
## Functions
## ------------------------------



##            generic functions

## is_valid
def is_valid(data, col_id, valid_values, thresh = .01):
    '''
    This function checks if the valid values
    are below a given threshold, marks an error in such case,
    and returns the dataset containing only valid values.
    '''
    valids = data[~data[col_id].isin(valid_values)]
    if len(valids) >= len(data[col_id]) * thresh: raise AssertionError
    return data[data[col_id].isin(valid_values)]

## rem_nulls
def rem_nulls(data, col_id, default_value = 0):
    '''
    This function removes null values.
    '''
    return data[col_id].apply(lambda x: default_value if pd.isnull(x) else x)



##            string functions

## rem_non_ascii
def rem_non_ascii(sentence):
    '''
    This function removes all non-ascii
    characters in sentence and sets every
    word to lower case.
    '''
    printable = set(string.printable)
    return filter(lambda x: x in printable, sentence).lower()



##          numeric functions

## is_numeric
def is_numeric(data, col_id):
    '''
    This function identifies if the given column
    is of type numeric.
    '''
    if not  np.issubdtype(data[col_id].dtype, np.number): raise AssertionError
    if data[col_id].isnull().sum()> 0: raise  AssertionError


## to_int
def to_int(item):
    try:
        new_item = int(item)
    except ValueError:
        new_item = 0
    return new_item



## not_null
def not_null(data, col_id):
    '''
    This function checks if the given column
    contains null types.
    '''
    if data[col_id].isnull().sum()> 0: raise  AssertionError


## get_positive
def get_positive(data, col_id, with_zero = False, thresh = .1):
    """
    This function return values greater  than 0 or greater equal zero
    depends on parameter with_zero
    """
    if with_zero:
        if len(data[data[col_id] < 0]) > (len(data[col_id]) * thresh) : raise AssertionError
        return data[data[col_id] >= 0]
    else:
        if len(data[data[col_id] <= 0]) >= (len(data[col_id]) * thresh) : raise AssertionError
        return data[data[col_id] > 0]


## clean_amount
def clean_amount(data, col_id, changes_id ,with_zero, not_null_values= True):
    """
    This function multiply col_id value with changes_id value
    and check if the column has null values
    """
    if not_null_values:
	not_null(data, col_id)
    else:
        data[col_id] = data[col_id].apply(lambda x: 0 if pd.isnull(x) else x)
    is_numeric(data, col_id)
    data[col_id] = get_positive(data, col_id, with_zero)
    data[col_id] = data.apply(transform_amount, args = (col_id, changes_id), axis = 1)
    return data

## transform_amount
def transform_amount(item, amount_column, change_column):
    amount = item[amount_column]
    change = item[change_column]
    return amount * change



##          normalize functions
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


def complete_paymentdate(item):
    try:
    	if np.isnan(item['PaymentDate']):
            return item['InvoiceDate'] + datetime.timedelta(days=30)
        else:
            return parser.parse(item['PaymentDate'])
    except TypeError:
        return parser.parse(item['PaymentDate'])


## date_valid
def date_valid(data, col_id, start_date, finish_date = None):
    '''
    This function checks if all dates of data are in range
    (start_date, finish_date)
    '''
    if data[col_id].min() <= start_date: raise AssertionError
    if not finish_date:
        finish_date = datetime.datetime.now()
    if data[col_id].max() >= finish_date: raise AssertionError


## search_currency
def search_currency(item, currency_column, date_column):
    '''
    This function search value of currency by date
    '''
    currency = item [currency_column]
    date  = item[date_column].date()
    if currency == CURRENCY_ERROR:
        return 1
    if currency == MX_CURRENCY:
        return 1
    fxrio = Fixerio(base='MXN')
    try:
        histo_dict = fxrio.historical_rates(date)
    except :
        time.sleep(3)
        histo_dict = fxrio.historical_rates(date)
    return histo_dict['rates'][currency]


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
        return CURRENCY_ERROR
    if max_value == mx_len:
        return MX_CURRENCY
    if max_value == usd_len:
         return USD_CURRENCY
    if max_value == euro_len:
        return EURO
    if max_value == other_search:
        return other_search.pop()
    return CURRENCY_ERROR

##    data = pd.read_csv('../data/export_V_TaxEntities.csv')
