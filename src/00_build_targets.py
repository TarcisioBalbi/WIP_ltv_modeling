import pandas as pd

def get_ltv(user, time_var='trns_ref',value_var='payment_value',period=180):
    ref =  user[time_var].min()
    ref_max = ref + pd.DateOffset(days=period)
    date_filter = (user[time_var]>ref) & (user[time_var]<=ref_max) 
    ltv = user[date_filter][value_var].sum()
    return pd.Series([ref,ltv])

data_path = 'data/'

orders = pd.read_csv(data_path+'bronze/olist_orders_dataset.csv')
order_items = pd.read_csv(data_path+'bronze/olist_order_items_dataset.csv')
order_payments = pd.read_csv(data_path+'bronze/olist_order_payments_dataset.csv')

customers = pd.read_csv(data_path+'bronze/olist_customers_dataset.csv')

valid_methods = ['credit_card', 'boleto', 'debit_card']

valid_order_payments = order_payments[order_payments['payment_type'].isin(valid_methods)].copy()
valid_order_payments = valid_order_payments.groupby('order_id').agg({'payment_value':['sum']}).reset_index()
valid_order_payments.columns = [x[0] for x in valid_order_payments.columns]

orders_filtered = orders.merge(valid_order_payments,on='order_id',how='inner') # we do an inner join since we are interested only in the orders paid with the valid methods.
orders_filtered = orders_filtered.merge(customers[['customer_id','customer_unique_id']],on='customer_id',how='inner') # we use inner so we only get traceable clients.

valid_status = ['delivered']
orders_filtered = orders_filtered[orders_filtered['order_status'].isin(valid_status)].copy()

orders_filtered = orders_filtered.sort_values(['customer_unique_id','order_purchase_timestamp'])

orders_filtered['client_order_number'] = orders_filtered.sort_values(['customer_unique_id','order_purchase_timestamp']).groupby(['customer_unique_id']).cumcount()+1
orders_filtered['order_purchase_timestamp'] = orders_filtered['order_purchase_timestamp'].apply(pd.to_datetime)
orders_filtered['trns_ref'] =  orders_filtered['order_purchase_timestamp'].dt.floor('d')
orders_filtered['time_lst_order'] = orders_filtered.groupby(['customer_unique_id'])['trns_ref'].diff(periods=1).dt.days
orders_filtered['time_nxt_order'] = orders_filtered.groupby(['customer_unique_id'])['trns_ref'].diff(periods=-1).dt.days.abs()

orders_filtered_grouped = orders_filtered.groupby(['customer_unique_id','trns_ref']).agg({'payment_value':'sum','client_order_number':'min'}).reset_index(drop=False)

# Target One and Done
df_oad = orders_filtered_grouped.copy()
df_oad['time_nxt_order'] = df_oad.groupby(['customer_unique_id'])['trns_ref'].diff(periods=-1).dt.days.abs() # we recalculate the recency now base on the day of the next transaction.

df_oad = df_oad[df_oad['client_order_number']==1].copy()
mask_oad = (df_oad['time_nxt_order'].isna() ) | (df_oad['time_nxt_order']>180 )

df_oad['one_and_done'] = 0
df_oad.loc[mask_oad,'one_and_done']=1
df_oad.rename(columns={'trns_ref':'ref'},inplace=True)

# Target LTV
df_ltv =orders_filtered_grouped.copy()

df_ltv['ref'] = df_ltv.groupby('customer_unique_id')['trns_ref'].transform('min')
date_filter = (df_ltv['trns_ref']>df_ltv['ref']) & (df_ltv['trns_ref']<=(df_ltv['ref']+ pd.DateOffset(days=180))) 
df_ltv = df_ltv[date_filter].groupby(['customer_unique_id','ref'])['payment_value'].sum().to_frame().reset_index(drop=False).rename(columns = {'payment_value':'LTV'})

# Saving
df_targets = df_oad[['customer_unique_id','ref','one_and_done']].copy()
df_targets = df_targets.merge(df_ltv,on=['customer_unique_id','ref'],how='left')

df_targets.to_parquet(data_path+'silver/df_targets.parquet')