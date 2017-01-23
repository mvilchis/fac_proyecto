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
## - Per day
## - Per Month (variance per month)
## - Per year  (variance per year)
## ------------------------------
get_income <- function(invoices){
    ## ---------------------
    ## Mean Per Client
    ## ---------------------
    c.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "IdClient")]
    c.income <- c.income[, max(V1), by = IdTaxEntity]
    names(c.income) <- c("IdTaxEntity", "max_mean_client")
    ## ---------------------
    ## Per day
    ## ---------------------
    d.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "PaymentDate")]
    d.income <- d.income[, max(V1), by = IdTaxEntity]
    names(d.income) <- c("IdTaxEntity", "max_mean_day")
    ## ---------------------
    ## Mean and Var Per month
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
    ## Mean and Var Per year
    ## ---------------------
    invoices$year  <- year(invoices$PaymentDate)
    y.income <- invoices[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "year")]
    y.income <- y.income[, max(V1), by = IdTaxEntity]
    names(y.income) <- c("IdTaxEntity", "max_mean_year")
    ## var
    y.v.income <- invoices[, var(TotalAmount), by = c("IdTaxEntity",
                                                     "year")]
    y.v.income <- y.v.income[, max(V1), by = IdTaxEntity]
    names(y.v.income) <- c("IdTaxEntity", "max_var_year")
    y.income   <- merge(y.income, y.v.income, by = "IdTaxEntity")
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
## - Per day
## - Per Month
## - Per year
## ------------------------------
get_outcome <- function(received){
    ## ---------------------
    ## Per day
    ## ---------------------
    d.outcome <- received[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "InvoiceDate")]
    d.outcome <- d.outcome[, max(V1), by = IdTaxEntity]
    names(d.outcome) <- c("IdTaxEntity", "max_mean_day")
    ## ---------------------
    ## Per month
    ## ---------------------
    received$month <- month(received$InvoiceDate)
    m.outcome <- received[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "month")]
    m.outcome <- m.outcome[, max(V1), by = IdTaxEntity]
    names(m.outcome) <- c("IdTaxEntity", "max_mean_month")
    ## var
    m.v.outcome <- received[, var(TotalAmount), by = c("IdTaxEntity",
                                                     "month")]
    m.v.outcome <- m.v.outcome[, max(V1), by = IdTaxEntity]
    names(m.v.outcome) <- c("IdTaxEntity", "max_var_month")
    m.outcome   <- merge(m.outcome, m.v.outcome, by = "IdTaxEntity")
    ## ---------------------
    ## Per year
    ## ---------------------
    received$year  <- year(received$InvoiceDate)
    y.outcome <- received[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "year")]
    y.outcome <- y.outcome[, max(V1), by = IdTaxEntity]
    names(y.outcome) <- c("IdTaxEntity", "max_mean_year")
    ## Var
    y.v.outcome <- received[, var(TotalAmount), by = c("IdTaxEntity",
                                                     "year")]
    y.v.outcome <- y.v.outcome[, max(V1), by = IdTaxEntity]
    names(y.v.outcome) <- c("IdTaxEntity", "max_var_year")
    y.outcome   <- merge(y.outcome, y.v.outcome, by = "IdTaxEntity")
    ## Merge together
    outcome <- merge(c.outcome, d.outcome, by = "IdTaxEntity")
    outcome <- merge(outcome, m.outcome, by = "IdTaxEntity")
    outcome <- merge(outcome, y.outcome, by = "IdTaxEntity")
    outcome
}

## ------------------------------
## get_items
## ------------------------------
## Get max mean income:
## (could be max sum)
## - Per day
## - Per Month
## - Per year
## ------------------------------
get_items <- function(){

}

## ------------------------------
## get_employees
## ------------------------------
## Get max mean income:
## (could be max sum)
## - Per day
## - Per Month
## - Per year
## ------------------------------
get_employees <- function(payrolls){
    ## ---------------------
    ## Per day
    ## ---------------------
    d.payroll <- payrolls[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "Date")]
    d.payroll <- d.payroll[, max(V1), by = IdTaxEntity]
    names(d.payroll) <- c("IdTaxEntity", "max_mean_day")
    ## ---------------------
    ## Per month
    ## ---------------------
    payrolls$month <- month(payrolls$Date)
    m.payroll <- payrolls[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "month")]
    m.payroll <- m.payroll[, max(V1), by = IdTaxEntity]
    names(m.payroll) <- c("IdTaxEntity", "max_mean_month")
    ## var
    m.v.payroll <- payrolls[, var(TotalAmount), by = c("IdTaxEntity",
                                                     "month")]
    m.v.payroll <- m.v.payroll[, max(V1), by = IdTaxEntity]
    names(m.v.payroll) <- c("IdTaxEntity", "max_var_month")
    m.payroll   <- merge(m.payroll, m.v.payroll, by = "IdTaxEntity")
    ## ---------------------
    ## Per year
    ## ---------------------
    payrolls$year  <- year(payrolls$Date)
    y.payroll <- payrolls[, mean(TotalAmount), by = c("IdTaxEntity",
                                                   "year")]
    y.payroll <- y.payroll[, max(V1), by = IdTaxEntity]
    names(y.payroll) <- c("IdTaxEntity", "max_mean_year")
    ## Var
    y.v.payroll <- payrolls[, var(TotalAmount), by = c("IdTaxEntity",
                                                     "year")]
    y.v.payroll <- y.v.payroll[, max(V1), by = IdTaxEntity]
    names(y.v.payroll) <- c("IdTaxEntity", "max_var_year")
    y.payroll   <- merge(y.payroll, y.v.payroll, by = "IdTaxEntity")
    ## Merge together
    payroll <- merge(c.payroll, d.payroll, by = "IdTaxEntity")
    payroll <- merge(payroll, m.payroll, by = "IdTaxEntity")
    payroll <- merge(payroll, y.payroll, by = "IdTaxEntity")
    payroll
}

##################################################
##################################################
#################### ANÁLISIS ####################
##################################################
##################################################

## ------------------------------
## Read in data
## ------------------------------
## Still missing:
## - Number of employees (we need the employee id).
## - Margination Index.
## - Max product outcome.
invoices <- fread('../data/final_clean/df_invoices.csv')
payrolls <- fread('../data/final_clean/df_payrolls.csv')
received <- fread('../data/final_clean/df_received.csv')

## ------------------------------
## Processs data
## ------------------------------
data.invoices <- get_income(invoices)
data.received <- get_outcome(received)
data.payrolls <- get_payrolls(payrolls)
## All data
all.data      <- merge(data.invoices,
                      data.received)
all.data      <- merge(all.data,
                      data.payrolls)
