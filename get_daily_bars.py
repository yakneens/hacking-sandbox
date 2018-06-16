import datetime
from ib_insync import *
import os
import time
import asyncio
from sqlalchemy import create_engine,insert,update
import psycopg2 as pg
import io
import pandas as pd
from sqlalchemy.schema import MetaData
import random 
from aioinflux import InfluxDBClient

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()
ib = IB()
ib.connect('127.0.0.1', 4002, clientId=int(random.random()*100))

client = InfluxDBClient(mode='blocking', db='stocks')

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contract_table = meta.tables["contracts"]

def to_df(my_bars, conId):
    bar_df = util.df(my_bars)
    bar_df['date'] = bar_df['date'].astype(pd.Timestamp)
    #bar_df = bar_df.set_index('date')
    bar_df = bar_df.loc[lambda df: df.volume > 0,:]
    bar_df['conId'] = conId
    return bar_df

def save_to_db(bars, conId):
    bars.to_sql("contract_daily_bars", engine, if_exists="append",index=False)
    result = connection.execute(contract_table.update().where(contract_table.c.conId==conId).values(daily_bar_load_date=datetime.datetime.now())) 

def save_to_influx(bars, contract):
    bars = bars.set_index('date')
    return client.write(bars, 
         measurement='contract_daily_bars', 
         symbol=contract.symbol,
         expiry=str(contract.lastTradeDateOrContractMonth.split(" ")[0]), 
         contractId=str(contract.conId),
         strike=str(contract.strike),
         right=contract.right,
         local_symbol=contract.localSymbol)

def main():
    #tasks = [asyncio.ensure_future(get_contracts(symbol)) for symbol in symbol_list]
    query = 'select * from contracts c join contract_ib_first_timestamp t on c."conId" = t."contractId" where t."firstTimestamp" is not null and c.daily_bar_load_date is null order by "lastTradeDateOrContractMonth" ASC'
    con_df = pd.read_sql(query, connection)
    tasks = []
    for index, row in con_df.iterrows():
        my_con = Option(conId=row.conId, exchange=row.exchange)
        num_days = (datetime.datetime.now(datetime.timezone.utc) - row.firstTimestamp).days + 1
        print("{} Processing contract {}".format(datetime.datetime.now(), row))
        try:
            my_bars = ib.reqHistoricalData(my_con, endDateTime='', durationStr='{} D'.format(num_days), barSizeSetting='8 hours', whatToShow='TRADES', useRTH=False, formatDate=2)
        except ValueError as e:
            print("Error getting historic bars for {} {}".format(this_contract.localSymbol, e))
            logging.error("Error getting historic bars for {}".format(this_contract.localSymbol))
            time.sleep(5)
            continue
        
        if my_bars:           
            bar_df = to_df(my_bars, row.conId)
            print("Saving to DB")
            save_to_db(bar_df, row.conId)
            print("Saving to Influx")
            save_to_influx(bar_df, row)
            time.sleep(5)
            
            
    

if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Execution time was: {}".format(str(time.time() - start_time)))