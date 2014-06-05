
from __future__ import print_function, division

import os
import time
import numpy,pandas,scipy

from mapReduce import initialize_reduce_store
from mapReduce import pool_mapper, mapper
from mapReduce import pool_reduce, reducer
from mapReduce import num_buckets

########################################################################
# Setup
########################################################################

base_dir = '../data/'
reduce_store_path = base_dir + 'reduce_store/'

map_file_list = [base_dir + 'transaction_chunks/' + str(r) + '.csv'
    for r in range(1,58+1)]
reduce_file_list = [reduce_store_path + str(r) + '.csv'
    for r in range(num_buckets)]


########################################################################
# mean_items_in_basket
########################################################################

from mapReduce import mean_items_in_basket, reduce_mean

def build_mean_items_in_basket

map_value_labels = ['mean_items_in_basket','n']
hash_key_labels  = ['id']
input_labels = ['id','date','category_brand_company']

# map
initialize_reduce_store(reduce_file_list,hash_key_labels+map_value_labels)
pool_mapper(map_file_list,reduce_store_path,
    mapper,mean_items_in_basket,
    map_value_labels,hash_key_labels,input_labels,
    num_processes=7)

# reduce
results = pool_reduce(reduce_file_list,reducer,reduce_mean,
    map_value_labels,hash_key_labels,
    num_processes=30,launch_offset=0)

# save feature
results.to_csv(base_dir+'features/'+'mean_items_in_basket.csv',index=False)

########################################################################
# mean_time_between_transactions
########################################################################

from features.mapReduce import mean_time_between_transactions, reduce_mean

map_value_labels = ['mean_time_between_transactions','n']
hash_key_labels  = ['id']
input_labels = ['id','date','dept']

# map
initialize_reduce_store(reduce_file_list,hash_key_labels+map_value_labels)
pool_mapper(map_file_list,reduce_store_path,
    mapper,mean_time_between_transactions,
    map_value_labels,hash_key_labels,input_labels,
    num_processes=7)

# reduce
results = pool_reduce(reduce_file_list,reducer,reduce_mean,
    map_value_labels,hash_key_labels,
    num_processes=30,launch_offset=0)

# save feature
results.to_csv(base_dir+'features/'+'mean_time_between_transactions.csv',index=False)
