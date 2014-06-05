from __future__ import print_function, division

import os
import time
from datetime import date, datetime, timedelta
from multiprocessing import Process, Lock, Queue, Pool
import numpy,pandas,scipy
import dataFrameTools

########################################################################
# Init
########################################################################

base_dir = 'data/'
reduce_store_path = base_dir + 'reduce_store/'

num_buckets=131
locks = {i:Lock() for i in xrange(num_buckets)}

map_file_list = [base_dir + 'chunks/' + str(r) + '.csv'
    for r in range(1,58+1)]
reduce_file_list = [reduce_store_path + str(r) + '.csv'
    for r in range(num_buckets)]


offers = pandas.read_csv(base_dir+'offers.csv')
offers = dataFrameTools.build_id_column(offers,['category','brand','company'],'offer_item_id')
offers = offers[['offer','company','quantity','category','offer_item_id','offer_measure']]

history = pandas.read_csv(base_dir+'trainHistory').drop(['repeattrips','repeater'], axis=1)
history['train_test'] = 1
history = pandas.concat([history,pandas.read_csv(base_dir+'testHistory')],axis=0)
history['train_test'] = history['train_test'].fillna(0)

d1 = pandas.DataFrame(history.loc[history['train_test']==1,'offer']).groupby('offer').size()
d1 = pandas.concat([d1,pandas.DataFrame(history.loc[history['train_test']==0,'offer']).groupby('offer').size()],axis=1)
history = pandas.merge(history,((~d1[0].isnull())*(~d1[1].isnull())).astype(int).reset_index(),on='offer',how='left')
history = history.rename(columns={0:'offer_in_train_and_test'})

history = history.merge(offers,on='offer',how='left')
history = history.rename(columns={'chain':'offer_chain',
    'market':'offer_market',
    'company':'offer_company',
    'category':'offer_category'})


########################################################################
# mapReduce
########################################################################


def initialize_reduce_store(reduce_file_list,columns):
    for file_path in reduce_file_list:
        with open(file_path,'w') as f:
            f.write(','.join(columns)+'\n')


def pool_mapper(map_file_list,reduce_file_path,
    mapper,map_transform,
    map_value_labels,hash_key_labels,input_labels,
    num_buckets=131,num_processes=15,launch_offset=2):
    '''
    Parameters
    ----------
    file_list     : list of files containing raw data chunks
    mapper        : function to be applied to each file in file_list
    keys          : function is applied to a subset of data columns listed
                    in keys
    num_processes : maximum number of processes running in pool
    launch_offset : time in seconds to offset launching processes in pool

    References
    ----------
    http://stackoverflow.com/questions/8533318/python-multiprocessing-pool-when-to-use-apply-apply-async-or-map
    '''
    # Do some stuff
    # hash.header contains names of columns to be hashed
    # hash.hash contains a dictionary mapping unique column names to hash values
    results = []
    pool = Pool(processes=num_processes)
    for file_path in map_file_list:
        pool.apply_async(mapper,
            args=(map_transform,file_path,reduce_file_path,
                map_value_labels,hash_key_labels,input_labels,
                num_buckets),
            callback=lambda x: results.extend(x))
        time.sleep(launch_offset)
    pool.close()
    pool.join()

def pool_reduce(reduce_file_list,
    reducer,reduce_transform,
    map_value_labels,hash_key_labels,
    num_processes=15,launch_offset=2):
    '''
    Parameters
    ----------
    file_list     : list of files containing raw data chunks
    mapper        : function to be applied to each file in file_list
    keys          : function is applied to a subset of data columns listed
                    in keys
    num_processes : maximum number of processes running in pool
    launch_offset : time in seconds to offset launching processes in pool

    Returns
    -------
    df : a dataframe with columns hash_key_labels + map_value_labels
         missing hash_keys are not in the result list

    References
    ----------
    http://stackoverflow.com/questions/8533318/python-multiprocessing-pool-when-to-use-apply-apply-async-or-map
    '''
    # Do some stuff
    # hash.header contains names of columns to be hashed
    # hash.hash contains a dictionary mapping unique column names to hash values
    global history
    results = []
    pool = Pool(processes=num_processes)
    for file_path in reduce_file_list:
        pool.apply_async(reducer,
            args=(reduce_transform,file_path,map_value_labels,hash_key_labels),
            callback=lambda x: results.append(x))
        time.sleep(launch_offset)
    pool.close()
    pool.join()
    # join results into a dataframe
    df = results[0]
    for result in results[1:]:
        if df.shape[0] > 0:
            if result.shape[0] > 0:
                df = pandas.concat([df,result],axis=0)
            else:
                continue
        else:
            df = result
    return df


def mapper(map_transform,in_path,out_path,map_keys,hash_keys,input_keys,num_buckets):
    # load data
    global history
    global locks
    data = pandas.read_csv(in_path)
    data = data.merge(history, on=['id'], how='left')[input_keys]

    # do something
    data = map_transform(data)

    # save results
    if data.shape[0]>0:
        hk = data[hash_keys]
        hk = hk.astype(int)
        hk = hk.astype(str)
        hk = hk.sum(axis=1)
        hk = hk.astype(int)
        data['hash_key'] = hk
        data['hash_key'] = data['hash_key'] % num_buckets
        for val in set(data['hash_key']):
            locks[val].acquire()
            out = data.loc[data['hash_key']==val,:]
            out = out[hash_keys+map_keys]
            out.to_csv(out_path+str(val)+'.csv',mode='a',header=False,index=False)
            locks[val].release()
    return []


def reducer(reduce_transform,in_path,map_value_labels,hash_key_labels):
    data = pandas.read_csv(in_path)
    data = reduce_transform(data,map_value_labels,hash_key_labels)
    return data

########################################################################
# reducers
########################################################################

def reduce_mean(data,map_value_labels,hash_key_labels):
    col = [col for col in map_value_labels if col != 'n'][0]
    group_size = data.groupby(hash_key_labels)[['n']].sum().reset_index()
    group_size.rename(columns={'n':('agg_'+col)},inplace=True)
    data = data.merge(group_size,on=hash_key_labels,how='left')
    data['agg_'+col] = data['n']/data['agg_'+col]
    data['agg_'+col] = data['agg_'+col]*data[col]
    data = data.groupby(hash_key_labels).sum().reset_index()
    data = data[hash_key_labels+['agg_'+col]]
    data.rename(columns={'agg_'+col:col},inplace=True)
    return data

def reduce_min(data,map_value_labels,hash_key_labels):
    data = data.groupby(hash_key_labels).min().reset_index()
    return data

def reduce_max(data,map_value_labels,hash_key_labels):
    data = data.groupby(hash_key_labels).max().reset_index()
    return data

def reduce_sum(data,map_value_labels,hash_key_labels):
    data = data.groupby(hash_key_labels).sum().reset_index()
    return data

def reduce_first(data,map_value_labels,hash_key_labels):
    data = data.groupby(hash_key_labels).agg(lambda x: x.iloc[0,:]).reset_index()
    return data

########################################################################
# maps
########################################################################

def mean_items_in_basket(data):
    data = data.groupby(['id','date'])[['category_brand_company']]
    data = data.size().reset_index()
    data = data.groupby(['id'])[[0]]
    average = data.mean().rename(columns={0:'mean_items_in_basket'})
    count   = pandas.DataFrame(data.size(),columns=['n'])
    data = pandas.concat([average,count],axis=1)
    data = data.reset_index()
    return data

def mean_time_between_transactions(data):
    data = data.groupby(['id','date'])[['dept']].size().reset_index().drop(0,axis=1)
    data['date'] = pandas.to_datetime(data['date'])
    data['lag']  = data['date']
    data = data.set_index('date')
    data['lag'] = data.groupby(['id'])['lag'].shift(1)
    data = data.reset_index()
    data['lag'] = data['date'] - data['lag']
    data['lag'] = data['lag']/numpy.timedelta64(timedelta(days=1))
    data = data.groupby(['id'])[['lag']]
    average = data.mean().rename(columns={'lag':'mean_time_between_transactions'})
    count   = pandas.DataFrame(data.size(),columns=['n'])
    data = pandas.concat([average,count],axis=1)
    data = data.reset_index()
    return data
