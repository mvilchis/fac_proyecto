from cleaners import *
UPDATE_DATA = True
if UPDATE_DATA:
    df_products = pd.read_csv('../../data/Products.csv')
    # IdProduct
    print "IdProduct"
    is_numeric(df_products, 'IdProduct')
    not_null(df_products, 'IdProduct')

    #IdTaxEntity
    print "IdTaxEntity"
    is_numeric(df_products, 'IdTaxEntity')
    not_null(df_products, 'IdTaxEntity')

    #Name
    print "Name"
    df_products['Name'] = df_products['Name'].fillna(MISSING)
    df_products['Name'] = df_products['Name'].apply(rem_non_ascii)
    #Description
    print "Description"
    df_products['Description'] =  df_products['Description'].fillna(MISSING)
    df_products['Description'] = df_products['Description'].apply(rem_non_ascii)

    #Unit
    print "Unit"
    df_products['Unit'] = df_products['Unit'].fillna(MISSING)
    df_products['Unit'] = df_products['Unit'].apply(rem_non_ascii)
    #Amounts
    print "Amounts"
    df_products['Changes'] = 1
    #Price
    print "Price"
    df_products = clean_amount(df_products, 'Price', 'Changes', with_zero = True, not_null_values = True)

    print "Tocsv"
    df_products.to_csv('../../data/df_products.csv',  header = 'column_names', index = False,  encoding='utf-8')
    df_tax = pd.DataFrame(df_products.groupby('IdTaxEntity').size())
    df_tax.to_csv('../../data/df_taxentity.csv',  header = 'column_names')
else:
    df_products = pd.read_csv('../../data/df_products')

