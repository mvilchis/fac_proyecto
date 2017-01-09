import csv
import pandas as pd
import numpy as np
from dateutil import parser
UPDATE_DATA = True

#################### Functions ##########################
def clean_payment(item):
    #Faltan 'NO IDENTIFICADO','ref43r43r4', nan
    if item in ['CONTADO', 'CONTADO ', 'Efectivo', 'Inmediato', 'PAGO EN UNA EXHIBICION', 'PAGO EN UNA SOLA EXHIBICION',
       'PAGO EN UNA SOLA EXHIBICI\xc3\x93N', 'Pago en una exhibicion',
       'Pago en una exhibici\xc3\xb3n', 'Pago en una sola exhibicion',
       'Pago en una sola exhibici\xc3\xb3n', ]:
        return 'CONTADO'
    if item in ['PAGO EN PARCIALIDADES',  'CRDITO', 'CREDITO', 'Credito', 'PARCIALIDAD 1 DE 4', 'PARCIALIDAD 1 DE 2']:
        return 'CREDITO'
    return item




############### Export_invoices  #######################

#IdTaxEntity  #IdClientes #Status (limpiar 0) #InvoiceDate (Formato de Fecha)  #Type  #Total amount #Payment Conditions
#Payment Day (/Revisar Monsi)
#PaymentT4ype se necesita limpiar a contado o credito

if UPDATE_DATA:
    invoices_all = pd.read_csv('./data/export_invoices.csv')
    invoices_df = invoices_all[['IdTaxEntity', 'IdClient', 'Status',
                           'InvoiceDate', 'Type', 'PaymentType',
                           'PaymentConditions', 'TotalAmount', 'PaymentDate' ]]
    #Clean status 0
    invoices_df = invoices_df[invoices_df.Status != 0]
    #InvoiceDate as date
    invoices_df['InvoiceDate'] = invoices_df['InvoiceDate'].apply(parser.parse)
    #Clean PaymentType
    np.unique(invoices_df['PaymentType'].values)
    #invoices_df = pd.read_csv('./data/data_invoices.csv')
else:
    invoices_df = pd.read_csv('./data/df_invoices.csv')





############### received invoices  #######################

#IdTaxentity  #Status, limpiar 0
#Paid  para: Tiempo que la mepresa tarda en pagar lo que adquiere

if UPDATE_DATA:
    received_all = pd.read_csv('./data/export_receivedinvoices.csv')
    received_df  = received_all[['IdTaxEntity', 'Status','InvoiceDate',
                                 'TotalAmount', 'Paid', 'PaymentDate', 'ReceivedDate'  ]]
    #Clean status 0
    received_df = received_df[received_df.Status != 0]
    #date string as date
    received_df['InvoiceDate'] = received_df['InvoiceDate'].apply(lambda x: parser.parse(x) if pd.notnull(x) else x)
    #######     Obviamente cuando no se  ha pagado es null
    received_df['PaymentDate'] = received_df['PaymentDate'].apply(lambda x: parser.parse(x) if pd.notnull(x) else x)
    received_df['ReceivedDate'] = received_df['ReceivedDate'].apply(lambda x: parser.parse(x) if pd.notnull(x) else x)
    received_df.to_csv('./data/df_received.csv',header ='column_names')
else:
    received_df = pd.read_csv('./data/df_received.csv')






##############       payrolls      #######################
# IdTaxEntity  # Id Employee # Date # total amount  # is paid
if UPDATE_DATA:
    payrolls_all = pd.read_csv('./data/export_payrolls.csv')
    payrolls_df = payrolls_all[['IdTaxEntity', 'IdEmployee', 'Status', 'Date', 'TotalAmount', 'IsPaid']]
    #date string as date
    payrolls_df['Date'] = payrolls_df['Date'].apply(lambda x: parser.parse(x) if pd.notnull(x) else x)
    payrolls_df.to_csv('./data/df_payrolls.csv')
else:
    payrolls_df = pd.read_csv('./data/df_payrolls.csv')


