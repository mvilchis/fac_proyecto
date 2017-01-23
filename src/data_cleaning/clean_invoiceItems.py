from cleaners import *
UPDATE_DATA = True
if UPDATE_DATA:
    df_invoiceItems = pd.read_csv('../../data/InvoiceItems.csv')
    #IdInvoiceItem
    print "IdInvoiceItem"
    is_numeric(df_invoiceItems, 'IdInvoiceItem')
    not_null(df_invoiceItems, 'IdInvoiceItem')

    #IdInvoice
    print "IdInvoice"
    is_numeric(df_invoiceItems, 'IdInvoice')
    not_null(df_invoiceItems, 'IdInvoice')

    #IdProduct
    print "IdProduct"
    is_numeric(df_invoiceItems, 'IdProduct')
    not_null(df_invoiceItems, 'IdProduct')

    df_invoiceItems['Changes'] = 1

    #Quantity
    print "Quantity"
    df_invoiceItems = clean_amount(df_invoiceItems, 'Quantity', 'Changes', with_zero = True, not_null_values = False)

    #Unit
    print "Unit"
    df_invoiceItems['Unit'] =                             df_invoiceItems['Unit'].fillna(MISSING)
    df_invoiceItems['Unit'] = df_invoiceItems['Unit'].apply(rem_non_ascii)

    #Description
    print "Description"
    df_invoiceItems['Description'] =                             df_invoiceItems['Description'].fillna(MISSING)
    df_invoiceItems['Description'] = df_invoiceItems['Description'].apply(rem_non_ascii)

    #IVA
    print "IVA"
    df_invoiceItems = clean_amount(df_invoiceItems, 'IVA', 'Changes', with_zero = True, not_null_values = False)

    #IvaPercentage
    print "IvaPercentage"
    df_invoiceItems = clean_amount(df_invoiceItems, 'IvaPercentage', 'Changes', with_zero = True, not_null_values = False)

    #UnitValue
    print "UnitValue"
    df_invoiceItems = clean_amount(df_invoiceItems, 'UnitValue', 'Changes', with_zero = True, not_null_values = False)

    #DiscountPercentage
    print "DiscountPercentage"
    df_invoiceItems = clean_amount(df_invoiceItems, 'DiscountPercentage', 'Changes', with_zero = True, not_null_values = False)

    #TotalValue
    print "TotalValue"
    df_invoiceItems = clean_amount(df_invoiceItems, 'TotalValue', 'Changes', with_zero = True, not_null_values = False)

    #SubTotalValue
    print "SubTotalValue"
    df_invoiceItems = clean_amount(df_invoiceItems, 'SubTotalValue', 'Changes', with_zero = True, not_null_values = False)

    #DiscountAmount
    print "DiscountAmount"
    df_invoiceItems = clean_amount(df_invoiceItems, 'DiscountAmount', 'Changes', with_zero = True, not_null_values = False)

    #DiscountIsPercentage
    print "DiscountIsPercentage"
    df_invoiceItems['DiscountIsPercentage'] = df_invoiceItems['DiscountIsPercentage'].fillna(0)

    df_invoiceItems.to_csv('../../data/df_invoiceItems.csv', index = False)
else:
    df_invoiceItems = pd.read_csv('../../data/df_invoiceItems.csv')
