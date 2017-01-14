import csv
import pandas as pd
import numpy as np
from dateutil import parser
import datetime

UPDATE_DATA = True
MISSING = 'missing'
MX_CURRENCY = 'MXN'
USD_CURRENCY = 'DLL'
EURO = 'EUR'

#################### Functions ##########################


'''
 Esto hay que automatizarlo, los tipos de cambio son dinamicos...
 hay que ademas hacer la consulta en la fecha que se llevo a cabo
 el pago de la facturacion, si no, podemos caer en ambiguedad.
'''

currency_dict = { 'BDT': 0.2756, 'BHD': 57.6423, 'BOB': 3.14 , 'CAD': 16.601, 'COP': 0.0074, 'CRC': 0.0391, 'CZK': 0.8564,
                 USD_CURRENCY: 21.7282, 'DZD': 15.4626, 'EEK': 1.8523, EURO: 23.1434, 'GBP': 26.6706, 'GHS': 5.1078, 'ISK': 0.1906,
                 'JPY': 0.1900, 'NGN': 0.069, 'NOK': 2.5562, 'PAB': 21.7368, 'SVC': 2.4898, 'THB': 0.6147, 'TZS': 0.0098, 'VEF': 2.1772,
                MX_CURRENCY : 1}


'''
 El problema de hacerlo hardcoded es que el proceso
 de limpieza de datos no se va a automatizar facilmente
 ¿Como tratar un dato nuevo con una codificacion que no
 se habia visto anteriormente?
'''
peso_list =  [ '$','CD MELCHOR MUZQUIZ', 'CULIACAN', 'M.N', 'M.N.', 'M.X.N.', 'M/N', 'MEX','MN', 'MNX', 'MONEDA',
              'MONEDA NACIONAL', 'MONEDA PESOS M.N.', 'MSXN', 'MX', 'MX PESO', 'MX PESOS', 'MXB', 'MXM', 'MXN',
              'MXN PUEBA', 'MXN.', 'MXNC', 'MXP', 'Mexico', 'Mxn', 'N ACIONAL','NACIOAL', 'NACIOINAL', 'NACION AL',
              'NACIONA', 'NACIONA;','NACIONAL', 'NACIONASL', 'NACIUONAL', 'NAL', 'NCIONAL', 'Nacional',
              'Nacional, Pesos',  'PESO','PESO M/N', 'PESO MEXICANO', 'PESO MEXICO', 'PESO MX', 'PESOS',
              'PESOS ,M.N.', 'PESOS M.-N.', 'PESOS M.N', 'PESOS M.N.','PESOS M.N.,', 'PESOS M/N', 'PESOS MEXICANO',
              'PESOS MEXICANOS','PESOS MEXICANSO', 'PESOS MEXICNOS', 'PESOS MEXICO', 'PESOS MEXIOCANOS', 'PESOS MN',
              'PESOS MONEDA NACIONA', 'PESOS MX', 'PESOS, M.N.', 'PESOS,M.N.', 'PESOSS', 'Peso', 'Peso MXN',
              'Peso Mexicano', 'Peso mexicano', 'Pesos', 'Pesos M.N.', 'Pesos MX','Pesos MXN', 'Pesos Mexicanos',
              'Pesos mexicano', 'Pesos mexicanos','Pesos.', 'Pess', 'P\xc3\x89SOS M.N.', 'SLP','TRANSFERENCIA','mx',
              'mxn', 'mxp','nacional', 'pESOS', 'peso', 'peso mexicano', 'pesos', 'pesos MX','pesos m.n.',
              'pesos mexicanos', 'pesos mx', 'pesos mxn','$ M.N.', '$MX', 'EFECTIVO', 'M', 'M. N.',
              'M. N. PESOS', 'M.N', 'M.N.','M.X.', 'M.X.N.', 'M/', 'M/N', 'M/N.', 'MEX', 'MEXICANA', 'MN',
              'MN ', 'MNX', 'MON', 'MONEDA NACIONAL', 'MX', 'MX MN', 'MXM', 'MXN','MXN  ', 'MXN Pesos',
              'MXN.', 'MXP', 'MXPESO', 'Mexicana', 'Mn','Mondea nacional', 'Moneda Nacional',
              'Moneda nacional', 'Mx','MxN', 'NAC', 'NACIONAL', 'NACIONAL, PESOS', 'NACIONALL', 'NACIONL',
              'NAL', 'Nacional', 'Nacional/Pesos', 'Nacionla', 'Ninguno','No Identificado', 'P',
              'PEMX', 'PES', 'PESO', 'PESO M.N.','PESO MEXICANO', 'PESO MEXICANO',
              'PESO MN','PESO MXN', 'PESOS', 'PESOS ', 'PESOS','PESOS (M.N.)', 'PESOS (MXN)',
              'PESOS 00/100 M. N.)','PESOS 00/100 M.N.', 'PESOS M N', 'PESOS M,N', 'PESOS M. N.',
              'PESOS M.N', 'PESOS M.N.', 'PESOS M/N', 'PESOS MEXICANOS', 'PESOS MEXICANOStipoCambio',
              'PESOS MN', 'PESOS MONEDA NACIONAL', 'PESOS MX', 'PESOS MXN', 'PESOS(M.N.)', 'PESOS(MN)',
              'PESOS(PESOS)','PESOS, M.N.', 'PESOS, MN', 'PESOS/MONEDA NACIONAL','PESOS_MEXICANOS', 'PEso',
              'PMX', 'PSM', 'Pes', 'Peso', 'Peso M.N','Peso M.N.', 'Peso MXN', 'Peso Mexicano', 'Peso Mx',
              'Peso MxN', 'Peso Mxn', 'Peso M\xc3\xa9xicano', 'Peso mexicano', 'Pesos','Pesos ', 'Pesos', 'Pesos (M.N.)',
              'Pesos (MXP)', 'Pesos M.N', 'Pesos M.N.', 'Pesos MX', 'Pesos MXN', 'Pesos Mexicanos',
              'Pesos Mexicanos M.N.', 'Pesos Mx', 'Pesos M\xc3\xa9xicanos', 'Pesos mexicanos', 'Pesos, M.N',
              'Pesos, MN', 'Pesos,mn', 'Pesos.', 'Pesos/M.N.', 'TRES MIL OCHOCIENTOS VEINTE PESOS 00/100 M.N.',
              'Tarjeta de Credito','eda=MXN', 'm.n','mexicana', 'mx', 'mxn', 'mxp', 'nacional', 'peso', 'peso mexicano',
              'pesos', 'pesos mexicanos', 'pesos, M.N.','EFVO']

dolar_list = ['DLL', 'DOLAR', 'DOLARES', 'DOLARES AMERICANOS','Dolar','Dolares', 'D\xc3\x93LARES', 'D\xc3\xb3lar',
              'D\xc3\xb3lar Americano', 'D\xc3\xb3lar estadounidense', 'D\xc3\xb3lares', 'USD',
              'D&oacute;lar estadounidense','D?LARES', 'DLL', 'DOL', 'DOLAR', 'DOLAR AMERICANO', 'DOLARES',
              'DOLARES AMERICANOS', 'DOLARES U.S.', 'DOLARES USD', 'Dolar', 'Dolar Americano', 'Dolares',
              'Dolares Estadounidences','Dolares usd', 'D\xc3\x93LAR', 'D\xc3\x93LARES', 'D\xc3\xb3lar',\
              'D\xc3\xb3lar Americano', 'D\xc3\xb3lar Americano DOF','D\xc3\xb3lares', 'D\xc3\xb3lares USD',
              'US', 'US DOLLAR', 'USD', 'USD DOLAR', 'USDL', 'dolares' ]
euro_list = ['EUR', 'EURO', 'EUROS']


def complete_paymentdate(item):
    try:
        if np.isnan(item['PaymentDate']):
            '''
            Falta poner condicion sobre si la factura esta activa,
            Segun recuerdo, si la factura no tenia PaymentDate pero
            estaba inactiva, no podíamos asumir nada.
            '''
            return item['InvoiceDate'] + datetime.timedelta(days=30) 
        else:
            return parser.parse(item['PaymentDate'])
    except TypeError:
        return parser.parse(item['PaymentDate'])

def trans_currency(item, field):
    '''
    field:
       SubTotalAmount, DiscountAmount, 
       TotalTax, TotalAmount
    '''
    currency = item ['Currency']
    value    = item[field]
    return currency_dict[currency]*value


def check_outlier_currency (serie):
    '''
    Creo que es buena idea marcar los outliers,
    pero creo que una buena forma de resolver este
    problema es haciendo un clasificador de tipos
    de monedas.
    '''
    value_list   = serie.values
    outlier_list = []
    for item in value_list:
        if not item in (currency_dict) and (item != MX_CURRENCY) and (item != USD_CURRENCY) and (item != EURO):
            outlier_list.append(item)
    return  outlier_list


def clean_currency(item):
    item = item.strip()
    if ( item in peso_list):
        return MX_CURRENCY
    elif (item in dolar_list):
        return USD_CURRENCY
    elif (item in euro_list):
        return EURO
    return item.upper()


def to_int(item):
    try:
        new_item = int(item)
    except ValueError:
        new_item = 0 
    return new_item


############### Export_invoices  #######################
########################################
#           Limpieza    de             #
#           Export_invoices            #
########################################
#Variables no relevantes : IdInvoice, IdBrachOffice, Serial, Folio, DiscountDescription, TaxEntityRFC,
#                         TaxEntityNeighborhood,
#                         TaxEntityCity, TaxEntityLocation, TaxEntityMunicipality, TaxEntityState, ClientLocation
#                         ClientCity, ClientMunicipality, ClientState ,ClientRFC (16 var)

#Variables relevantes: IdTaxEntity (Id de los clientes de facturama)
#                      IdClient (Id de los clientes de los clientes)
#                      Status (Solo trabajar con 1)
#                      InvoiceDate (Fecha en que se expide la factura)
#                      Type (Se usa la tabla InvoicesType)
#                      Currency (Hacer la conversión)
#                      PaymentMethod (Ingreso, egreso y traslado)
#                      SubTotalAmount, DiscountAmount, TotalTax, TotalAmount (Calcular salidas efectivas)
#                      TaxEntityTaxName, TaxEntityZipCode (Buscar en donde se encuentra y evaluar la zona)
#                      ClientTaxName, ClientZipCode (Buscar en donde se encuentra y evaluar la zona de sus clientes)
#                      Paid, PaymentDate (Facturas pagadas) (17 var)


if UPDATE_DATA:
    all_invoices = pd.read_csv('./data/csv_factu/V_Invoices.csv')
    df_invoices = all_invoices[['IdTaxEntity', 'IdClient', 'Status', 'InvoiceDate','Type',
                               'Currency', 'PaymentMethod', 'SubTotalAmount', 'DiscountAmount',
                                'TotalTax', 'TotalAmount', 'TaxEntityTaxName', 'TaxEntityZipCode',
                               'ClientTaxName', 'ClientZipCode', 'Paid', 'PaymentDate']]
    # IdTaxEntity
    # Id numero y no nulo, pueden ser repetidos
    if not  np.issubdtype(df_invoices['IdTaxEntity'].dtype, np.number): raise AssertionError
    if pd.isnull(df_invoices['IdTaxEntity']).all(): raise  AssertionError

    # IdClient Falta hacer group_by idTaxEntity para saber el numero de clientes y la salida de efectivo x c/u
    if not  np.issubdtype(df_invoices['IdClient'].dtype, np.number): raise AssertionError
    if pd.isnull(df_invoices['IdClient']).all(): raise  AssertionError

    # Status (Hay 1,2,0) de 2 son 60 registros, (quitados)
    df_invoices = df_invoices[df_invoices['Status']==1]

    #InvoiceDate from date string to date
    if pd.isnull(df_invoices['InvoiceDate']).all(): raise  AssertionError
    df_invoices['InvoiceDate'] = df_invoices['InvoiceDate'].apply(parser.parse)
    #Check  outliers
    date_copy = df_invoices['InvoiceDate'].copy()
    date_copy.sort()
    if date_copy[0] <= parser.parse('2000-12-30'): raise  AssertionError

    #Type
    if pd.isnull(df_invoices['Type']).all(): raise  AssertionError
    if not  np.issubdtype(df_invoices['Type'].dtype, np.number): raise AssertionError

    #Currency
    df_invoices['Currency'] = df_invoices['Currency'].apply(lambda x: MX_CURRENCY if pd.isnull(x) else x)
    df_invoices['Currency'] = df_invoices['Currency'].apply(clean_currency)
    list_outlier = check_outlier_currency(df_invoices['Currency'])
    if len(list_outlier)>= (len(df_invoices['Currency'])*0.10): raise  AssertionError
    df_invoices = df_invoices[~df_invoices['Currency'].isin(list_outlier)]
    if pd.isnull(df_invoices['Currency']).all(): raise  AssertionError

    #PaymentMethod
    if pd.isnull(df_invoices['PaymentMethod']).all(): raise  AssertionError
    valid_paymentmethod = ['egreso', 'ingreso', 'traslado']
    outlier = df_invoices[~df_invoices['PaymentMethod'].isin(valid_paymentmethod)]
    if len(outlier) >= (len(df_invoices['PaymentMethod'])*0.10): raise  AssertionError
    df_invoices = df_invoices[df_invoices['PaymentMethod'].isin(valid_paymentmethod)]

    #SubTotalAmount
    if pd.isnull(df_invoices['SubTotalAmount']).all(): raise  AssertionError
    if not  np.issubdtype(df_invoices['SubTotalAmount'].dtype, np.number): raise AssertionError
    if len(df_invoices[df_invoices['SubTotalAmount'] <= 0])  >= (len(df_invoices['SubTotalAmount'])*0.10): raise  AssertionError
    df_invoices = df_invoices[df_invoices['SubTotalAmount'] > 0]

    #DiscountAmount
    df_invoices['DiscountAmount'] = df_invoices['DiscountAmount'].apply(lambda x: 0 if pd.isnull(x) else x)
    if pd.isnull(df_invoices['DiscountAmount']).all(): raise  AssertionError
    if not  np.issubdtype(df_invoices['DiscountAmount'].dtype, np.number): raise AssertionError
    if len(df_invoices[df_invoices['DiscountAmount'] < 0])  >= (len(df_invoices['DiscountAmount'])*0.10): raise  AssertionError
    df_invoices = df_invoices[df_invoices['DiscountAmount'] >= 0]

    #TotalTax
    df_invoices['TotalTax'] = df_invoices['TotalTax'].apply(lambda x: 0 if pd.isnull(x) else x)
    if not  np.issubdtype(df_invoices['TotalTax'].dtype, np.number): raise AssertionError
    if len(df_invoices[df_invoices['TotalTax'] < 0])  >= (len(df_invoices['TotalTax'])*0.10): raise  AssertionError
    df_invoices = df_invoices[df_invoices['TotalTax'] >= 0]

    #TotalAmount
    if pd.isnull(df_invoices['TotalAmount']).all(): raise  AssertionError
    if not  np.issubdtype(df_invoices['TotalAmount'].dtype, np.number): raise AssertionError
    if len(df_invoices[df_invoices['TotalAmount'] <= 0])  >= (len(df_invoices['TotalAmount'])*0.10): raise  AssertionError
    df_invoices = df_invoices[df_invoices['TotalAmount'] > 0]

    #TaxEntityTaxName
    df_invoices['TaxEntityTaxName'] = df_invoices['TaxEntityTaxName'].apply(lambda x: MISSING if pd.isnull(x) else x)

    #TaxEntityZipCode
    df_invoices['TaxEntityZipCode'] = df_invoices['TaxEntityZipCode'].apply(lambda x: 0 if pd.isnull(x) else x)
    df_invoices['TaxEntityZipCode'] = df_invoices['TaxEntityZipCode'].apply(to_int)
    df_invoices['TaxEntityZipCode'] = df_invoices['TaxEntityZipCode'].astype(int)

    #ClientTaxName
    df_invoices['ClientTaxName'] = df_invoices['ClientTaxName'].apply(lambda x: MISSING if pd.isnull(x) else x)

    #ClientZipCode
    df_invoices['ClientZipCode'] = df_invoices['ClientZipCode'].apply(lambda x: 0 if pd.isnull(x) else x)
    df_invoices['ClientZipCode'] = df_invoices['ClientZipCode'].apply(to_int)
    df_invoices['ClientZipCode'] = df_invoices['ClientZipCode'].astype(int)

    #Paid (Borrar)
    df_invoices['Paid'] = True

    #PaymentDate
    df_invoices['PaymentDate'] = df_invoices.apply(complete_paymentdate, axis=1)

    # Conversion
    df_invoices['SubTotalAmount'] = df_invoices.apply(trans_currency_subtotal,
                                                      args=('SubTotalAmount'), axis=1)
    df_invoices['DiscountAmount'] = df_invoices.apply(trans_currency_discount,
                                                      args=('DiscountAmount'), axis=1)
    df_invoices['TotalTax'] = df_invoices.apply(trans_currency_totaltax,
                                                      args=('TotalTax'), axis=1)
    df_invoices['TotalAmount'] = df_invoices.apply(trans_currency_total,
                                                      args=('TotalAmount'), axis=1)


    df_invoices.to_csv('./data/df_invoices.csv',header ='column_names')
else:
    df_invoices = pd.read_csv('./data/df_invoices.csv')




############### received invoices  #######################
########################################
#           Limpieza    de             #
#           received_invoices          #
########################################
#Variables no relevantes: IdReceivedInvoice, Serial, Folio, TaxEntityRFC, IssuerRFC, IssuerTaxName, ReceivedDate
#Variables relevantes: IdTaxEntity
#                      Status
#                      InvoiceDate
#                      InvoiceType
#                      SubTotalAmount, TotalTax, TotalAmount para calcular el efectivo neto
#                      TaxEntityTaxName  (Buscar en donde se encuentra y evaluar la zona)
#                      Currency (Hacer la conversión)
#                      PaymentDate, Paid 11 var
if UPDATE_DATA:
    all_received = pd.read_csv('./data/csv_factu/export_V_ReceivedInvoices.csv')
    df_received = all_received[['IdTaxEntity','Status','InvoiceDate', 'InvoiceType', 'SubTotalAmount',
                                'TotalTax', 'TotalAmount', 'TaxEntityTaxName','Currency','PaymentDate', 'Paid']]
    # IdTaxEntity
    # Id numero y no nulo, pueden ser repetidos
    if not  np.issubdtype(df_received['IdTaxEntity'].dtype, np.number): raise AssertionError
    if df_received['IdTaxEntity'].isnull().sum() >0: raise  AssertionError

    # Status (Hay 1,0)
    df_received = df_received[df_received['Status']==1]


    #InvoiceDate
    if df_received['InvoiceDate'].isnull().sum() >0: raise  AssertionError
    df_received['InvoiceDate'] = df_received['InvoiceDate'].apply(parser.parse)
    #Check  outliers minimo es 2011-07-07
    date_copy = df_received['InvoiceDate'].copy()
    date_copy.sort()
    if date_copy[0] <= parser.parse('2000-12-30'): raise  AssertionError

    #InvoiceType
    # Hay 286918 nulos a ingreso
    df_received['InvoiceType'] = df_received['InvoiceType'].apply(lambda x: 'ingreso' if pd.isnull(x) else x)

    #Subtotalamount
    if df_received['SubTotalAmount'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_received['SubTotalAmount'].dtype, np.number): raise AssertionError
    if len(df_received[df_received['SubTotalAmount'] <= 0])  >= (len(df_received['SubTotalAmount'])*0.10): raise  AssertionError
    df_received = df_received[df_received['SubTotalAmount'] > 0]

    #TotalTax
    df_received['TotalTax'] = df_received['TotalTax'].apply(lambda x: 0 if pd.isnull(x) else x)
    if not  np.issubdtype(df_received['TotalTax'].dtype, np.number): raise AssertionError
    if len(df_received[df_received['TotalTax'] < 0])  >= (len(df_received['TotalTax'])*0.10): raise  AssertionError
    df_received = df_received[df_received['TotalTax'] >= 0]

    #TotalAmount
    if df_received['TotalAmount'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_received['TotalAmount'].dtype, np.number): raise AssertionError
    if len(df_received[df_received['TotalAmount'] <= 0])  >= (len(df_received['TotalAmount'])*0.10): raise  AssertionError
    df_received = df_received[df_received['TotalAmount'] > 0]

    #TaxEntityTaxName (Hacer join)
    df_received['TaxEntityTaxName'] = df_received['TaxEntityTaxName'].apply(lambda x: MISSING if pd.isnull(x) else x)

    #Currency
    df_received['Currency'] = df_received['Currency'].apply(lambda x: MX_CURRENCY if pd.isnull(x) else x)
    df_received['Currency'] = df_received['Currency'].apply(clean_currency)
    list_outlier = check_outlier_currency( df_received['Currency'])
    if len(list_outlier)>= (len(df_received['Currency'])*0.10): raise  AssertionError
    df_received = df_received[~df_received['Currency'].isin(list_outlier)]

    ##Paid (Borrar)
    df_received['Paid'] = True

    # PaymentDate
    df_received['PaymentDate'] = df_received.apply(complete_paymentdate, axis=1)

    # conversion
    df_received['SubTotalAmount'] = df_received.apply(trans_currency_subtotal,
                                                      args=('SubTotalAmount'), axis=1)
    df_received['TotalTax'] = df_received.apply(trans_currency_totaltax,
                                                      args=('TotalTax'), axis=1)
    df_received['TotalAmount'] = df_received.apply(trans_currency_total,
                                                      args=('TotalAmount'), axis=1)
    df_received.to_csv('./data/df_received.csv',header ='column_names')
else:
    df_received = pd.read_csv('./data/df_received.csv')



##############       payrolls      #######################
########################################
#           Limpieza    de             #
#           payrolls                   #
########################################

#Variables no relevantes: IdPayroll,u'Folio', 'IsPaid', 'CancelationDate
#Variables relevantes: IdTaxEntity
#                      Status
#                      Date
#                      Perceptions', ExtraHours, SubTotalAmount, Deductions ,ISR
#                      DiscountAmount  , TotalAmount  para calcular el efectivo neto


if UPDATE_DATA:
    all_payrolls = pd.read_csv('./data/csv_factu/export_V_Payrolls.csv')
    df_payrolls = all_payrolls[['IdTaxEntity','Status', 'Date', 'Perceptions',
                                'ExtraHours', 'SubTotalAmount', 'Deductions' ,'ISR', 'DiscountAmount',
                                'TotalAmount']]
    # IdTaxEntity
    # Id numero y no nulo, pueden ser repetidos
    if not  np.issubdtype(df_payrolls['IdTaxEntity'].dtype, np.number): raise AssertionError
    if df_payrolls['IdTaxEntity'].isnull().sum() >0: raise  AssertionError

    # Status (Hay 1,0,4) son 25 registros con 4, se quita
    df_payrolls = df_payrolls[df_payrolls['Status']==1]

    #Date
    if df_payrolls['Date'].isnull().sum() >0: raise  AssertionError
    df_payrolls['Date'] = df_payrolls['Date'].apply(parser.parse)
    #Check  outliers minimo es  2014-01-13 20:09:47
    date_copy = df_payrolls['Date'].copy()
    date_copy.sort()
    #if date_copy[0] <= parser.parse('2000-12-30'): raise  AssertionError

    #Perceptions
    if df_payrolls['Perceptions'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_payrolls['Perceptions'].dtype, np.number): raise AssertionError
    if len(df_payrolls[df_payrolls['Perceptions'] <= 0])  >= (len(df_payrolls['Perceptions'])*0.10): raise  AssertionError
    df_payrolls = df_payrolls[df_payrolls['Perceptions'] > 0]

    #ExtraHours no negativos
    if df_payrolls['ExtraHours'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_payrolls['ExtraHours'].dtype, np.number): raise AssertionError
    if len(df_payrolls[df_payrolls['ExtraHours'] < 0])  >= (len(df_payrolls['ExtraHours'])*0.10): raise  AssertionError
    df_payrolls = df_payrolls[df_payrolls['ExtraHours'] >= 0]

    #Subtotalamount  revisar, muchos con 0
    if df_payrolls['SubTotalAmount'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_payrolls['SubTotalAmount'].dtype, np.number): raise AssertionError
    if len(df_payrolls[df_payrolls['SubTotalAmount'] < 0])  >= (len(df_payrolls['SubTotalAmount'])*0.10): raise  AssertionError
    df_payrolls = df_payrolls[df_payrolls['SubTotalAmount'] >= 0]

    #Deductions no negativos
    if df_payrolls['Deductions'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_payrolls['Deductions'].dtype, np.number): raise AssertionError
    if len(df_payrolls[df_payrolls['Deductions'] < 0])  >= (len(df_payrolls['Deductions'])*0.10): raise  AssertionError
    df_payrolls = df_payrolls[df_payrolls['Deductions'] >= 0]

    #ISR no negativo (Hay negativos)
    if df_payrolls['ISR'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_payrolls['ISR'].dtype, np.number): raise AssertionError
    if len(df_payrolls[df_payrolls['ISR'] < 0])  >= (len(df_payrolls['ISR'])*0.10): raise  AssertionError
    df_payrolls = df_payrolls[df_payrolls['ISR'] >= 0]

    #DiscountAmount no negativo (Hay negativos)
    if df_payrolls['DiscountAmount'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_payrolls['DiscountAmount'].dtype, np.number): raise AssertionError
    if len(df_payrolls[df_payrolls['DiscountAmount'] < 0])  >= (len(df_payrolls['DiscountAmount'])*0.10): raise  AssertionError
    df_payrolls = df_payrolls[df_payrolls['DiscountAmount'] >= 0]

    #TotalAmount
    if df_payrolls['TotalAmount'].isnull().sum() >0: raise  AssertionError
    if not  np.issubdtype(df_payrolls['TotalAmount'].dtype, np.number): raise AssertionError
    if len(df_payrolls[df_payrolls['TotalAmount'] <= 0])  >= (len(df_payrolls['TotalAmount'])*0.10): raise  AssertionError
    df_payrolls = df_payrolls[df_payrolls['TotalAmount'] > 0]




    df_payrolls.to_csv('./data/df_payrolls.csv',header ='column_names')
else:
    df_payrolls = pd.read_csv('./data/df_payrolls.csv')

