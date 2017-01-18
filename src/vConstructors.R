#! /bin/python


## ------------------------------
# Authors:
##  - Miguel Vilchis
##  - Luis Román
## ------------------------------
## Description:
## This script contains multiple utileries
## for building the general prospects
## and institutions tables.
## ------------------------------
# Usage:


##

## ------------------------------
## Libraries
## ------------------------------
library(data.table)

## ------------------------------
## Functions
## ------------------------------

get_clients <- function(clients, invoices){
    n_clients      <- clients[, .N, by = IdTaxEntity]
    amount_clients <- invoices[, sum(Currency), by = by = IdTaxEntity]
}

get_providers <- function(providers, invoices){
    clients[, .N, by = IdTaxEntity]
}


## ------------------------------
## Read in data
## ------------------------------
invoices   <- fread('../data/clean_data/df_invoices.csv')
providers  <- fread('../data/export_V_Providers.csv')
clients    <- fread('../data/export_v_clients.csv')
