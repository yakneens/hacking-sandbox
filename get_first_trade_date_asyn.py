import datetime
from ib_insync import *
import os
import time
import asyncio
from sqlalchemy import create_engine
import psycopg2 as pg
import io
import pandas as pd
import random
from sqlalchemy.schema import MetaData

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=int(random.random()*100))

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp_async"]

sema = asyncio.Semaphore(2)

async def get_timestamp(contract):
    my_con = Option(conId=contract.conId, exchange=contract.exchange)
    try:
        async with sema:
            first_date = await ib.reqHeadTimeStampAsync(my_con, "TRADES", False,2)
            print("Completed {} {}".format(contract.conId, str(first_date)))
    except ValueError:
        first_date = None
    
    if not isinstance(first_date, datetime.datetime):
        first_date = None
        
    return (contract.conId, first_date)   

async def main():
    #tasks = [asyncio.ensure_future(get_contracts(symbol)) for symbol in symbol_list]
    con_df = pd.read_sql_table("contracts", connection)
    ts_df = pd.read_sql_table("contract_ib_first_timestamp_async", connection)
    timestamped_contracts = ts_df['contractId'].values
    tasks = []
    batch_size = 500
    for ind in range(0, len(con_df), batch_size):
        print("Processing batch # {}".format(str(int(ind/batch_size) + 1)))
        for index, row in con_df[ind:ind+batch_size].iterrows():
            if row.conId not in timestamped_contracts:
                tasks.append(asyncio.ensure_future(get_timestamp(row)))
            else:
                print("{} already exists, skipping.".format(row.conId))
        
        results = await asyncio.gather(*tasks)
        print("Saving") 
        for  (conId, first_date) in results:
            result = connection.execute(contract_timestamp_table.insert().values(contractId=conId, firstTimestamp=first_date))
    
    #pd.DataFrame(results, columns=['contract_id', 'first_timestamp']).to_sql("contract_ib_first_timestamp_async_test", engine, if_exists="append")

if __name__ == '__main__':
    start_time = time.time()
    IB.run(main())
    print("Execution time was: {}".format(str(time.time() - start_time)))