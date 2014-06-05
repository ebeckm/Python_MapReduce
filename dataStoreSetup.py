from __future__ import print_function, division
from multiprocessing import Process, Lock, Queue, Pool

import os
import time
import numpy,pandas,scipy

import dataFrameTools
import functions

########################################################################
# chunkSource
########################################################################

base_dir = 'data/'
file_list = [base_dir + 'chunks/' + str(r) + '.csv'
    for r in range(1,58+1)]


def get_file_list(path):
    '''
    Description
    -----------
    Return a list containing names of files in a given directory
    '''
    return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]


def chunk_source_table(in_path, out_path, unique_id,
                       read_size=3000000, write_size=30000000):
    '''
    Description
    -----------
    Read in a large file and break it into chunks.  The data is panel data,
    use unique key to group observations so they are not distributed accross
    files.
    '''
    chunk_reader = pandas.read_csv(in_path, chunksize=read_size)
    out_chunk = []
    out_count = 0
    for chunk in chunk_reader:
        if type(out_chunk) == list:
            out_chunk = chunk
            continue
        if out_chunk.values.nbytes > write_size:
            out_count += 1
            print('Writing chunk ',out_count)
            tail = chunk[unique_id]==chunk[unique_id].iloc[-1,:]
            tail = tail.product(axis=1).astype(bool)
            chunk_tail = chunk.loc[tail,:]
            chunk_head = chunk.loc[~tail,:]
            out_chunk = pandas.concat([out_chunk,chunk_head])
            out_chunk = out_chunk.set_index('id')
            out_chunk.to_csv(out_path + str(out_count)+'.csv')
            out_chunk = chunk_tail
        else:
            out_chunk = pandas.concat([out_chunk,chunk])
    return [str(r) for r in range(1,out_count+1)]


def main():
    in_path = base_dir + 'transactions'
    out_path = base_dir + 'chunks/'
    unique_id = ['id','date'] # Potential bug... should ['market','chain'] be in here as well?
    chunk_source_table(in_path,out_path,unique_id)

