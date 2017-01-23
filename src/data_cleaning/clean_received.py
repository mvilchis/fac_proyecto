from cleaners import *
UPDATE_DATA = True

############### received invoices  #######################

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
    all_received = pd.read_csv('../../data/ReceivedInvoices.csv' )
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

    #Status
    print "Status"
    is_numeric(df_received, 'Status')
    check_valid(df_received, 'Status', [0,1])
    df_received =  df_received[df_received['Status'] == 1]

    #InvoiceDate
    print "InvoiceDate"
    not_null(df_received, 'InvoiceDate')
    df_received['InvoiceDate'] = df_received['InvoiceDate'].apply(parser.parse)
    check_date_valid(df_received, 'InvoiceDate', parser.parse('2000-12-30'))

    # InvoiceType
    print "InvoiceType"
    df_received['InvoiceType'] = df_received['InvoiceType'].fillna('ingreso')
    df_received['InvoiceType'] = df_received['InvoiceType'].apply(rem_non_ascii)

    #TaxEntityTaxName
    print "TaxEntityTaxName"
    df_received['TaxEntityTaxName'] = df_received['TaxEntityTaxName'].fillna(MISSING)
    df_received['TaxEntityTaxName'] = df_received['TaxEntityTaxName'].apply(rem_non_ascii)

    # PaymentDate
    print "PaymentDate"
    df_received.loc[:,'PaymentDate'] = df_received.apply(complete_paymentdate, axis=1)

    #Currency
    print "Currency"
    df_received['Currency'] = df_received['Currency'].fillna(MX_CURRENCY)
    df_received['Currency'] = df_received['Currency'].apply(normalize_currency)
    df_received['Changes'] = df_received.apply(search_currency, args = ('Currency', 'InvoiceDate'), axis = 1)

    #SubtotalAmount
    print "SubTotalAmount"
    df_received = clean_amount(df_received, 'SubTotalAmount', 'Changes', with_zero = True, not_null_values = False)

    #TotalTax
    print "TotalTax"
    df_received = clean_amount(df_received, 'TotalTax', 'Changes',with_zero = True, not_null_values = False)

    #TotalAmount
    print "TotalAmount"
    df_received = clean_amount(df_received, 'TotalAmount', 'Changes', with_zero = True, not_null_values = False)

    df_received.to_csv('../../data/df_received.csv', header = 'column_names', index = False)
else:
    df_received = read_csv('../../data/df_received.csv')


