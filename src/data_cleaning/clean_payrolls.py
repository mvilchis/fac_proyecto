from cleaners import *
UPDATE_DATA = True

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
    all_payrolls = pd.read_csv('../../data/Payrolls.csv')
    df_payrolls = all_payrolls[['IdPayroll','IdTaxEntity','Status', 'Date', 'Perceptions',
                                 'ExtraHours', 'SubTotalAmount', 'Deductions' ,'ISR', 'DiscountAmount',
                                 'TotalAmount']]

    # IdReceivedInvoice
    print "IdReceivedInvoice"
    is_numeric(df_payrolls, 'IdPayroll')
    not_null(df_payrolls, 'IdPayroll')

    # IdTaxEntity
    print "IdTaxEntity"
    is_numeric(df_payrolls, 'IdTaxEntity')
    not_null(df_payrolls, 'IdTaxEntity')

    #Status
    print "Status"
    is_numeric(df_payrolls, 'Status')
    check_valid(df_payrolls, 'Status', [0,1])
    df_payrolls = df_payrolls[df_payrolls['Status'] == 1]

    #Date
    print "Date"
    not_null(df_payrolls, 'Date')
    df_payrolls['Date'] = df_payrolls['Date'].apply(parser.parse)
    check_date_valid(df_payrolls, 'Date', parser.parse('2000-12-30'))

    df_payrolls['Changes'] = 1

    #SubtotalAmount
    print "Subtotal"
    df_payrolls = clean_amount(df_payrolls, 'SubTotalAmount', 'Changes', with_zero = True, not_null_values = False)

    #Perceptions
    print "Perceptions"
    df_payrolls = clean_amount(df_payrolls, 'Perceptions', 'Changes',with_zero = True, not_null_values = False)

    #ExtraHours
    print "ExtraHours"
    df_payrolls = clean_amount(df_payrolls, 'ExtraHours', 'Changes',with_zero = True, not_null_values = False)

    #Deductions
    print "Deductions"
    df_payrolls = clean_amount(df_payrolls, 'Deductions', 'Changes',with_zero = True, not_null_values = False)

    #ISR
    print "ISR"
    df_payrolls = clean_amount(df_payrolls, 'ISR', 'Changes',with_zero = True, not_null_values = False)

    #DiscountAmount
    print "DiscountAmount"
    df_payrolls = clean_amount(df_payrolls, 'DiscountAmount', 'Changes',with_zero = True, not_null_values = False)

    #TotalAmount
    print "TotalAmount"
    df_payrolls = clean_amount(df_payrolls, 'TotalAmount', 'Changes', with_zero = True, not_null_values = False)

    df_payrolls.to_csv('../../data/df_payrolls.csv', header = 'column_names')
else:
    df_payrolls = read_csv('../../data/df_payrolls.csv')



