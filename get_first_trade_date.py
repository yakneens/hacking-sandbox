import datetime
from ib_insync import *
import os
import time
import asyncio
from sqlalchemy import create_engine
import psycopg2 as pg
import io
import pandas as pd
from sqlalchemy.schema import MetaData
import sys

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()
ib = IB()
ib.connect('127.0.0.1', 4002, clientId=111)

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]

#symbol_list = ["AZO","MNK"]
#sema = asyncio.Semaphore(10)
def get_timestamp(contract):
    my_con = Option(conId=contract.conId, exchange=contract.exchange)
    first_date = ib.reqHeadTimeStamp(my_con, "TRADES", False,2)
    print("Completed {} {}".format(contract.conId, str(first_date)))

    
    if not isinstance(first_date, datetime.datetime):
        raise ValueError
        
    return (contract.conId, first_date)   

def main():
    #tasks = [asyncio.ensure_future(get_contracts(symbol)) for symbol in symbol_list]
    con_df = pd.read_sql_table("contracts", connection).sort_values(axis=0,by='lastTradeDateOrContractMonth')
    ts_df = pd.read_sql_table("contract_ib_first_timestamp", connection)
    timestamped_contracts = ts_df['contractId'].values
    tasks = []
    for index, row in con_df.iterrows():
        print("{} {}".format(index, row.conId))
        if row.conId not in timestamped_contracts:
            try:
                (conId, first_date) = get_timestamp(row)
                result = connection.execute(contract_timestamp_table.insert().values(contractId=row.conId, firstTimestamp=first_date))
                #time.sleep(5)
            except ValueError as e:
                print(e)
                continue
        else:
            print("{} {}, timestamp exists, skipping.".format(index, row.conId))
            
    

if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Execution time was: {}".format(str(time.time() - start_time)))