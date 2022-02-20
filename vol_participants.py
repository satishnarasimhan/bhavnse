# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 10:06:03 2022

@author: Satish Narasimhan
"""

import pandas as pd
from datetime import datetime
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/market_analytics')
tablename = 'vol_participants'

d = pd.date_range(start = '2/11/2022' , end = '2/18/2022' , freq = 'B')

for day in d:
    try:
        dformat = datetime.strftime(day, '%d%m%Y')
        url = 'https://archives.nseindia.com/content/nsccl/fao_participant_vol_' +dformat+'.csv'
        data = pd.read_csv(url, skiprows = 1)
        data = data.drop(data.index[4])
        data.insert(0,'Date', day)
        data.columns = [c.strip() for c in data.columns.values.tolist()]
        data.to_sql(name = tablename, con = engine, index = False, if_exists = 'append')
        print('Data updated in table for' + str(d))
    except Exception:
        print('Error in' + str(d))

engine.dispose()
print('Done')      
        