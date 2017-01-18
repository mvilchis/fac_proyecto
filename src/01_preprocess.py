from cleaners import *
UPDATE_DATA = True

############### Export_invoices  #######################
########################################
#           Limpieza    de             #
#           Export_invoices            #
########################################
#payrolls 0 para cancelados,
#1,2,3, aun no esta emitida la nomina.
#4 cuando ya esta emitida.
#Variables no relevantes :  IdBrachOffice, Serial, Folio, DiscountDescription, TaxEntityRFC,
#                         TaxEntityNeighborhood,
#                         TaxEntityCity, TaxEntityLocation, TaxEntityMunicipality, TaxEntityState, ClientLocation
#                         ClientCity, ClientMunicipality, ClientState ,ClientRFC (16 var)

#Variables relevantes: IdInvoice (Id de las facturas)
#                      IdTaxEntity (Id de los clientes de facturama)
#                      IdClient (Id de los clientes de los clientes)
#                      Status (Solo trabajar con 1)
#                      InvoiceDate (Fecha en que se expide la factura)
#                      Type (Se usa la tabla InvoicesType)
#                      Currency (Hacer la conversin)
#                      PaymentMethod (Ingreso, egreso y traslado)
#                      SubTotalAmount, DiscountAmount, TotalTax, TotalAmount (Calcular salidas efectivas)
#                      TaxEntityTaxName, TaxEntityZipCode (Buscar en donde se encuentra y evaluar la zona)
#                      ClientTaxName, ClientZipCode (Buscar en donde se encuentra y evaluar la zona de sus clientes)
#                      Paid, PaymentDate (Facturas pagadas) (17 var)

if UPDATE_DATA:
    all_invoices = pd.read_csv('../data/V_Invoices.csv')
    df_invoices = all_invoices[['IdInvoice', 'IdTaxEntity', 'IdClient', 'Status', 'InvoiceDate','Type',
                            'Currency', 'PaymentMethod', 'SubTotalAmount', 'DiscountAmount',
                            'TotalTax', 'TotalAmount', 'TaxEntityTaxName', 'TaxEntityZipCode',
                            'ClientTaxName', 'ClientZipCode', 'Paid', 'PaymentDate']]


    is_numeric(df_invoices, 'IdInvoice')
    not_null(df_invoices, 'IdInvoice')

    #IdTaxEntity
    is_numeric(df_invoices, 'IdTaxEntity')
    not_null(df_invoices, 'IdTaxEntity')

    #IdClient
    is_numeric(df_invoices, 'IdClient')
    not_null(df_invoices, 'IdClient')

    #Status
    is_numeric(df_invoices, 'Status')
    is_valid(df_invoices, 'Status', [0,1])
    df_invoices = df_invoices[df_invoices['Status'] == 1]

    #InvoiceDate
    not_null(df_invoices, 'InvoiceDate')
    df_invoices['InvoiceDate'] = df_invoices['InvoiceDate'].apply(parser.parse)
    date_valid(df_invoices, 'InvoiceDate', parser.parse('2000-12-30'))

    # Type
    not_null(df_invoices, 'Type')
    is_numeric(df_invoices, 'Type')

    #TaxEntityTaxName
    #df_invoices['TaxEntityTaxName'] = rem_non_ascii(df_invoices['TaxEntityTaxName'])
    df_invoices['TaxEntityTaxName'] = df_invoices['TaxEntityTaxName'].apply(lambda x:        MISSING if pd.isnull(x) else x)

    #TaxEntityZipCode
    df_invoices['TaxEntityZipCode'] = df_invoices['TaxEntityZipCode'].apply(lambda x: 0 if   pd.isnull(x) else x)
    df_invoices['TaxEntityZipCode'] = df_invoices['TaxEntityZipCode'].apply(to_int)
    df_invoices['TaxEntityZipCode'] = df_invoices['TaxEntityZipCode'].astype(int)

    #ClientTaxName
    #df_invoices['ClientTaxName'] = rem_non_ascii(df_invoices['TaxEntityTaxName'])
    df_invoices['ClientTaxName'] = df_invoices['ClientTaxName'].apply(lambda x: MISSING if   pd.isnull(x) else x)

    #ClientZipCode
    df_invoices['ClientZipCode'] = df_invoices['ClientZipCode'].apply(lambda x: 0 if pd.     isnull(x) else x)
    df_invoices['ClientZipCode'] = df_invoices['ClientZipCode'].apply(to_int)
    df_invoices['ClientZipCode'] = df_invoices['ClientZipCode'].astype(int)

    # PaymentDate
    df_invoices['PaymentDate'] = df_invoices.apply(complete_paymentdate, axis=1)

    #Currency
    df_invoices['Currency'] = df_invoices['Currency'].apply(lambda x: MX_CURRENCY if pd.isnull(x) else x)
    df_invoices['Currency'] = df_invoices['Currency'].apply(normalize_currency)

    df_invoices['Changes'] = df_invoices.apply(search_currency, args = ('Currency', 'InvoiceDate'), axis = 1)


    #SubtotalAmount
    clean_amount(df_invoices, 'SubTotalAmount', 'Changes', with_zero = True, not_null_values = True)

    #TotalTax
    clean_amount(df_invoices, 'TotalTax', 'Changes',with_zero = True, not_null_values = False)

    #DiscountAmount
    clean_amount(df_invoices, 'TotalTax', 'Changes',with_zero = True, not_null_values = False)

    #TotalAmount
    clean_amount(df_invoices, 'TotalAmount', 'Changes', with_zero = False, not_null_values = True)

    df_invoices.to_csv('../data/df_invoices.csv', header = 'column_names')
else:
    df_invoices = pd.read_csv('../data/df_invoices.csv')



############### received invoices  #######################
########################################
#           Limpieza    de             #
#           received_invoices          #
########################################
#Variables no relevantes: IdReceivedInvoice, Serial, Folio, TaxEntityRFC, IssuerRFC,       IssuerTaxName, ReceivedDate
#Variables relevantes: IdReceivedInvoice,
#                      IdTaxEntity
#                      Status
#                      InvoiceDate
#                      InvoiceType
#                      SubTotalAmount, TotalTax, TotalAmount para calcular el efectivo     neto
#                      TaxEntityTaxName  (Buscar en donde se encuentra y evaluar la zona)
#                      Currency (Hacer la conversi0n)
#                      PaymentDate, Paid 11 var

if UPDATE_DATA:
    all_received = pd.read_csv('../data/csv_factu/export_V_ReceivedInvoices.csv')
    df_received = all_received[['IdReceivedInvoice','IdTaxEntity','Status','InvoiceDate', 'InvoiceType',       'SubTotalAmount',
                                 'TotalTax', 'TotalAmount', 'TaxEntityTaxName','Currency',  'PaymentDate', 'Paid']]


    # IdReceivedInvoice
    is_numeric(df_received, 'IdReceivedInvoice')
    not_null(df_received, 'IdReceivedInvoice')

    # IdTaxEntity
    is_numeric(df_received, 'IdTaxEntity')
    not_null(df_received, 'IdTaxEntity')

    #Status
    is_numeric(df_received, 'Status')
    is_valid(df_received, 'Status', [0,1])
    df_received = df_received[df_received['Status'] == 1]

    #InvoiceDate
    not_null(df_received, 'InvoiceDate')
    df_received['InvoiceDate'] = df_received['InvoiceDate'].apply(parser.parse)
    date_valid(df_received, 'InvoiceDate', parser.parse('2000-12-30'))

    # InvoicesType
    #not_null(df_received, 'InvoicesType')
    #is_numeric(df_received, 'InvoiceType')

    #TaxEntityTaxName
    #df_invoices['TaxEntityTaxName'] = rem_non_ascii(df_invoices['TaxEntityTaxName'])
    df_received['TaxEntityTaxName'] = df_received['TaxEntityTaxName'].apply(lambda x:        MISSING if pd.isnull(x) else x)
    # PaymentDate
    df_received['PaymentDate'] = df_received.apply(complete_paymentdate, axis=1)
    #Currency
    df_received['Currency'] = df_received['Currency'].apply(lambda x: MX_CURRENCY if pd.isnull(x) else x)
    df_received['Currency'] = df_received['Currency'].apply(normalize_currency)

    df_received['Changes'] = df_received.apply(search_currency, args = ('Currency', 'InvoiceDate'), axis = 1)


    #SubtotalAmount
    clean_amount(df_received, 'SubTotalAmount', 'Changes', with_zero = True, not_null_values = False)

    #TotalTax
    clean_amount(df_received, 'TotalTax', 'Changes',with_zero = True, not_null_values = False)

    #TotalAmount
    clean_amount(df_received, 'TotalAmount', 'Changes', with_zero = True, not_null_values = False)

    df_received.to_csv('../data/df_received.csv', header = 'column_names')
else:
    df_received = read_csv('../data/df_received.csv')



############### received invoices  #######################
########################################
#           Limpieza    de             #
#           received_invoices          #
########################################
#Variables no relevantes: IdReceivedInvoice, Serial, Folio, TaxEntityRFC, IssuerRFC,       IssuerTaxName, ReceivedDate
#Variables relevantes: IdReceivedInvoice,
#                      IdTaxEntity
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
    all_payrolls = pd.read_csv('../data/csv_factu/export_V_Payrolls.csv')
    df_payrolls = all_payrolls[['IdPayroll','IdTaxEntity','Status', 'Date', 'Perceptions',
                                 'ExtraHours', 'SubTotalAmount', 'Deductions' ,'ISR', 'DiscountAmount',
                                 'TotalAmount']]

    # IdReceivedInvoice
    is_numeric(df_payrolls, 'IdPayroll')
    not_null(df_payrolls, 'IdPayroll')

    # IdTaxEntity
    is_numeric(df_payrolls, 'IdTaxEntity')
    not_null(df_payrolls, 'IdTaxEntity')

    #Status
    is_numeric(df_payrolls, 'Status')
    is_valid(df_payrolls, 'Status', [0,1])
    df_payrolls = df_payrolls[df_payrolls['Status'] == 1]

    #Date
    not_null(df_payrolls, 'Date')
    df_payrolls['Date'] = df_payrolls['Date'].apply(parser.parse)
    date_valid(df_payrolls, 'Date', parser.parse('2000-12-30'))

    df_payrolls['Changes'] = 1
    #SubtotalAmount
    clean_amount(df_payrolls, 'SubTotalAmount', 'Changes', with_zero = True, not_null_values = False)

    #Perceptions
    clean_amount(df_payrolls, 'Perceptions', 'Changes',with_zero = True, not_null_values = False)

    #ExtraHours
    clean_amount(df_payrolls, 'ExtraHours', 'Changes',with_zero = True, not_null_values = False)

    #Deductions

    clean_amount(df_payrolls, 'Deductions', 'Changes',with_zero = True, not_null_values = False)

    #ISR

    clean_amount(df_payrolls, 'ISR', 'Changes',with_zero = True, not_null_values = False)

    #DiscountAmount

    clean_amount(df_payrolls, 'DiscountAmount', 'Changes',with_zero = True, not_null_values = False)
    #TotalAmount
    clean_amount(df_payrolls, 'TotalAmount', 'Changes', with_zero = True, not_null_values = False)

    df_payrolls.to_csv('../data/df_payrolls.csv', header = 'column_names')
else:
    df_payrolls = read_csv('../data/df_payrolls.csv')
