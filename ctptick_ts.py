#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
#  import h5py
import pandas as pd
import tables
import tstables as ts

#from WindPy import *


#w.start()


class Tick(tables.IsDescription):
    timestamp = tables.Int64Col(pos=0)
    last_price = tables.Float64Col(pos=1)
    qty = tables.Int64Col(pos=2)
    bid_price1 = tables.Float64Col(pos=3)
    bid_qty1 = tables.Int64Col(pos=4)
    ask_price1 = tables.Float64Col(pos=5)
    ask_qty1 = tables.Int64Col(pos=6)


def process_csv(month):
    root = 'd:/datas/ctp/FutAC_TickKZ_CTP_Daily_{}'.format(month)
    with tables.open_file('d:/ts_futures.h5','a') as hdf_file:
        for d in os.listdir(root):
            if not hdf_file.__contains__('/{}'.format(d)):
                hdf_file.create_group('/', d, '')
            for f in os.listdir(os.path.join(root, d)):
                print d,f
                instrument_with_out_date = f.split('_')[0]
                store_key = ''
                if u'次主力连续' in instrument_with_out_date.decode('cp936'):
                    store_key = instrument_with_out_date.decode('cp936').rstrip(u'次主力连续') + '_minor'
                elif u'主力连续' in instrument_with_out_date.decode('cp936') :
                    store_key = instrument_with_out_date.decode('cp936').rstrip(u'主力连续') + '_major'
                else:
                    store_key = instrument_with_out_date
                df = pd.read_csv(os.path.join(root, d, f), names=['last_price', 'qty', 'update_time',
                                  'update_millisec', 'bid_price1', 'bid_qty1', 'ask_price1', 'ask_qty1', 'action_date'],
                           usecols=[4, 11, 20, 21, 22, 23, 24, 25, 43],
                           skiprows=1,
                           encoding='cp936')
                store_key = store_key.lower()
                df['datetime'] = pd.to_datetime(df.action_date.apply(str) + ' ' + df.update_time + '.' + df.update_millisec.apply(str))
                del df['update_time']
                del df['update_millisec']
                del df['action_date']
                df = df[['datetime', 'last_price', 'qty', 'bid_price1','bid_qty1', 'ask_price1', 'ask_qty1']]
                df.set_index('datetime', inplace=True)
                df.index = df.index.sort_values()
                if hdf_file.__contains__('/{}/{}'.format(d, store_key)):
                    ts = hdf_file.get_node('/{}/{}'.format(d, store_key))._f_get_timeseries()
                    ts.append(df)
                else:
                    ts = hdf_file.create_ts('/{}'.format(d), store_key, Tick)
                    ts.append(df)
        #df_all = pd.concat(dfs, ignore_index=True)
        #df_all.update_time = df_all.update_time.str.encode('utf-8')
        #df_all.to_hdf('futures_{}.h5'.format(d), 'tick')


def main():
    process_csv(sys.argv[1])

if __name__ == '__main__':
    sys.exit(main())
