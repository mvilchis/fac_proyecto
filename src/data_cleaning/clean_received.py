from cleaners import *
UPDATE_DATA = True


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
    all_received = pd.read_csv('../../data/ReceivedInvoices.csv')
    df_received = all_received[['IdReceivedInvoice','IdTaxEntity','Status',
                                'InvoiceDate'      ,'InvoiceType', 'SubTotalAmount',
                                'TotalTax'         ,'TotalAmount', 'TaxEntityTaxName',
                                'Currency'         ,  'PaymentDate', 'Paid']]


    # IdReceivedInvoice
    print "IdReceivedInvoice"
    is_numeric(df_received, 'IdReceivedInvoice')
    not_null(df_received, 'IdReceivedInvoice')

    # IdTaxEntity
    print "IdTaxEntity"
    is_numeric(df_received, 'IdTaxEntity')
    not_null(df_received, 'IdTaxEntity')

    print "Status"
    #Status
    is_numeric(df_received, 'Status')
    check_valid(df_received, 'Status', [0,1])
    df_received =  df_received[df_received['Status'] == 1]

    print "InvoiceDate"
    #InvoiceDate
    not_null(df_received, 'InvoiceDate')
    df_received['InvoiceDate'] = df_received['InvoiceDate'].apply(parser.parse)
    not_null(df_received, 'InvoiceDate')
    check_date_valid(df_received, 'InvoiceDate', parser.parse('2000-12-30'))
    print "InvoiceType"
    # InvoiceType
    df_received['InvoiceType'] = df_received['InvoiceType'].apply(lambda x: 'Ingreso' if pd.isnull(x) else x)

    #TaxEntityTaxName
    df_received['TaxEntityTaxName'] = df_received['TaxEntityTaxName'].apply(lambda x: MISSING if pd.isnull(x) else x)
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

    df_received.to_csv('../../data/df_received.csv', header = 'column_names', index = False)
else:
    df_received = read_csv('../../data/df_received.csv')


