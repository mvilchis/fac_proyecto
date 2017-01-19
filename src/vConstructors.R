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

## ------------------------------
## get_income
## ------------------------------
## Get max mean income:
## (could be max sum)
## - Per client
## - Per day   (variance per day)
## - Per Month (variance per month)
## - Per year  (variance per year)
## ------------------------------
get_income <- function(invoices){
    ## ---------------------
    ## Per Client
    ## ---------------------
    c.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "IdClient")]
    c.income <- c.income[, max(V1), by = IdTaxEntity]
    names(c.income) <- c("IdTaxEntity", "max_mean_client")
    ## Per day
    d.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "PaymentDate")]
    d.income <- d.income[, max(V1), by = IdTaxEntity]
    names(d.income) <- c("IdTaxEntity", "max_mean_day")
    ## var
    d.v.income <- invoices[, var(TotalAmount), by = c("IdTaxEntity",
                                                     "PaymentDate")]
    d.v.income <- d.v.income[, max(V1), by = IdTaxEntity]
    names(d.v.income) <- c("IdTaxEntity", "max_var_month")
    d.income   <- merge(d.income, d.v.income, by = "IdTaxEntity")

    ## ---------------------
    ## Mean Per month
    ## ---------------------
    invoices$month <- month(invoices$PaymentDate)
    ## mean
    m.income   <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                      "month")]
    m.income   <- m.income[, max(V1), by = IdTaxEntity]
    ## var
    m.v.income <- invoices[, var(TotalAmount), by = c("IdTaxEntity",
                                                     "month")]
    m.v.income <- m.v.income[, max(V1), by = IdTaxEntity]
    names(m.v.income) <- c("IdTaxEntity", "max_var_month")
    m.income   <- merge(m.income, m.v.income, by = "IdTaxEntity")
    ## ---------------------
    ## Per year
    ## ---------------------
    invoices$year  <- year(invoices$PaymentDate)
    y.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "year")]
    y.income <- y.income[, max(V1), by = IdTaxEntity]
    names(y.income) <- c("IdTaxEntity", "max_mean_year")
    ## Merge together
    income <- merge(c.income, d.income, by = "IdTaxEntity")
    income <- merge(income, m.income, by = "IdTaxEntity")
    income <- merge(income, y.income, by = "IdTaxEntity")
    income
}

## ------------------------------
## get_outcome
## ------------------------------
## Get max mean income:
## (could be max sum)
## - Per client
## - Per day
## - Per Month
## - Per year
## ------------------------------
get_outcome <- function(received){
    ## Per day
    d.outcome <- received[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "InvoiceDate")]
    d.outcome <- d.outcome[, max(V1), by = IdTaxEntity]
    names(d.outcome) <- c("IdTaxEntity", "max_mean_day")
    ## Per month
    received$month <- month(received$InvoiceDate)
    m.outcome <- received[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "month")]
    m.outcome <- m.outcome[, max(V1), by = IdTaxEntity]
    names(m.outcome) <- c("IdTaxEntity", "max_mean_month")
    ## Per year
    received$year  <- year(received$InvoiceDate)
    y.outcome <- received[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "year")]
    y.outcome <- y.outcome[, max(V1), by = IdTaxEntity]
    names(y.outcome) <- c("IdTaxEntity", "max_mean_year")
    ## Merge together
    outcome <- merge(c.outcome, d.outcome, by = "IdTaxEntity")
    outcome <- merge(outcome, m.outcome, by = "IdTaxEntity")
    outcome <- merge(outcome, y.outcome, by = "IdTaxEntity")
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
received <- fread('../data/final_clean/df_received.csv')
payrolls <- fread('../data/final_clean/df_payrolls.csv')
received <- fread('../data/final_clean/df_received.csv')
