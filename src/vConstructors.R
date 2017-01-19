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
##
## ------------------------------
## Get max mean income:
## (could be max sum)
## - Per client
## - Per day
## - Per Month
## - Per year
## ------------------------------

get_outcome <- function(invoices){
    ## Per Client
    c.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "IdClient")]
    c.income <- c.income[, max(V1), by = IdTaxEntity]
    names(c.income) <- c("IdTaxEntity", "max_mean_client")
    ## Per day
    d.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "PaymentDate")]
    d.income <- d.income[, max(V1), by = IdTaxEntity]
    names(d.income) <- c("IdTaxEntity", "max_mean_day")
    ## Per month
    invoices$month <- month(invoices$PaymentDate)
    m.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "month")]
    m.income <- m.income[, max(V1), by = IdTaxEntity]
    names(m.income) <- c("IdTaxEntity", "max_mean_month")
    ## Per year
    invoices$year  <- year(invoices$PaymentDate)
    y.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "year")]
    y.income <- y.income[, max(V1), by = IdTaxEntity]
    names(y.income) <- c("IdTaxEntity", "max_mean_year")
    ## Merge together
    outcome <- merge(c.income, d.income, by = "IdTaxEntity")
    outcome <- merge(outcome, m.income, by = "IdTaxEntity")
    outcome <- merge(outcome, y.income, by = "IdTaxEntity")
    outcome
}

get_providers <- function(providers){
    n_providers <- providers[, .N, by = 'IdTaxEntity']
    n_providers
}

get_employees <- function(payrolls){
    n_employees <- payrolls[, .N, by = IdTaxEntity]
    n_employees
}
## ------------------------------
## Read in data
## ------------------------------
invoices <- fread('../data/final_clean/df_invoices.csv')
payrolls <- fread('../data/final_clean/df_payrolls.csv')
received <- fread('../data/final_clean/df_received.csv')
