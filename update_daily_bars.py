import datetime
import random
import time
import asyncio
import pandas as pd
from aioinflux import InfluxDBClient
from ib_insync import *
from sqlalchemy import create_engine, update
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import sessionmaker
import pytz
import os

IB_PORT = os.environ.get('IB_PORT')
print(IB_PORT)
if not IB_PORT:
    IB_PORT = '4002'
#IB_PORT = '7496'
engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

client = InfluxDBClient(mode='blocking', db='stocks')

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contract_table = meta.tables["contracts"]

def onError(reqId, errorCode, errorString, contract):
    print("ERROR",reqId, errorCode, errorString)
    if errorCode == 200 and errorString == 'No security definition has been found for the request':
        this_contract = session.query(contract_table).filter_by(conId=contract.conId).first()
        if this_contract:
            expiry = datetime.datetime.strptime(this_contract.lastTradeDateOrContractMonth.split(" ")[0], '%Y%m%d')
            if expiry < datetime.datetime.now():
                print("Contract expired, setting expiry flag")
                stmt = update(contract_table).where(contract_table.c.conId == contract.conId).values(expired=True)
                session.execute(stmt)
                session.commit()
    elif errorCode == 1102:
        print("Restarting after outage")


def connect_ib():
    ib = IB()
    ib.setCallback("error", onError)
    ib.RequestTimeout = 300
    ib.connect('127.0.0.1', IB_PORT, clientId=12)
    return ib


def to_df(my_bars, conId):
    bar_df = util.df(my_bars)
    #bar_df['date'] = bar_df['date'].astype(pd.Timestamp)
    bar_df['date'] = bar_df['date'].apply(lambda d: d.to_pydatetime())
    # bar_df = bar_df.set_index('date')
    bar_df = bar_df.loc[lambda df: df.volume > 0, :]
    bar_df['conId'] = conId
    bar_df['addedOn'] = datetime.datetime.now()
    return bar_df

def filter_existing(my_bars, conId):
    query = f'select date from contract_daily_bars where "conId" = {conId}'
    existing_dates = pd.read_sql(query, connection, parse_dates={'date':{'utc':True}})['date']
    return my_bars.query('date not in @existing_dates')




def save_to_db(bars, conId):
    bars['date'] = bars['date'].astype(pd.Timestamp)
    bars.to_sql("contract_daily_bars", engine, if_exists="append", index=False)

def update_load_date(conId):
    result = connection.execute(contract_table.update().where(contract_table.c.conId == conId).values(
        daily_bar_load_date=datetime.datetime.now()))


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
    ib = connect_ib()
    start_cutoff = 0
    end_cutoff = 1
    query = 'select c2.*, b."lastDate" from contracts c2 join (select max(date) as "lastDate", ' \
            'b."conId", max(volume) as max_volume ' \
            'from contract_daily_bars b join contracts c on b."conId" = c."conId" ' \
            'where DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now()) >= {} and ' \
            'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  <= {} and ' \
            'c.expired is not true and ' \
            'DATE_PART(\'day\', now() - c."daily_bar_load_date" :: timestamp with time zone) > 0 ' \
            'group by b."conId") b on c2."conId" = b."conId" order by c2."lastTradeDateOrContractMonth", c2.priority, b.max_volume DESC;'.format(start_cutoff, end_cutoff)

    con_df = pd.read_sql(query, connection)
    num_rows = len(con_df)
    tasks = []
    for index, row in con_df.iterrows():


        if row.secType == "OPT":
            my_con = Option(conId=row.conId, exchange=row.exchange)
        elif row.secType == "FOP":
            my_con = FuturesOption(conId=row.conId, exchange=row.exchange)
        else:
            print(f"Unknown security type {row.secType}")
            exit(1)

        try:
            ib.qualifyContracts(my_con)
        except ValueError:
            continue

        num_days = (datetime.datetime.now(datetime.timezone.utc) - row.lastDate).days + 1
        print(f"{index}/{num_rows} {datetime.datetime.now()} Processing contract {row.localSymbol} {row.lastTradeDateOrContractMonth} {row.lastDate}")
        try:
            my_bars = ib.reqHistoricalData(my_con, endDateTime='', durationStr='{} D'.format(num_days),
                                           barSizeSetting='8 hours', whatToShow='TRADES', useRTH=False, formatDate=2)
        except ValueError as e:
            print("Error getting historic bars for {} {}".format(row.localSymbol, e))
            continue

        if my_bars:
            bar_df = to_df(my_bars, row.conId)
            bar_df = filter_existing(bar_df, row.conId)

            if not bar_df.empty:
                print("Saving to DB")
                save_to_db(bar_df, row.conId)
                #print("Saving to Influx")
                #save_to_influx(bar_df, row)
            else:
                print("No updates")

        update_load_date(row.conId)

if __name__ == '__main__':
    start_time = time.time()
    while True:
        try:
            main()
            break
        except asyncio.TimeoutError as e:
            print("Asyncio timeout")
            time.sleep(60)
            continue
        except OSError as e:
            print("Can't connect. Retrying after 60 seconds")
            time.sleep(60)
            continue
        except:
            print("General error. Retrying after 60 seconds")
            time.sleep(60)
            continue
    print("Execution time was: {}".format(str(time.time() - start_time)))

#ib.run()