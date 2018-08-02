import datetime
import random
import time
import asyncio
import pandas as pd
from aioinflux import InfluxDBClient
from ib_insync import *
from sqlalchemy import create_engine, update, TIMESTAMP
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import sessionmaker

import os

IB_PORT = os.environ.get('IB_PORT')

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
equity_contracts = meta.tables["equity_contracts"]


def onError(reqId, errorCode, errorString, contract):
    print("ERROR", reqId, errorCode, errorString)
    if errorCode == 200 and errorString == 'No security definition has been found for the request':
        print("Bad contract")
    elif errorCode == 1102:
        print("Restarting after outage")


def connect_ib():
    ib = IB()
    ib.RequestTimeout = 300
    ib.errorEvent += onError
    ib.connect('127.0.0.1', IB_PORT, clientId=11)
    return ib

def to_df(my_bars, conId, symbol, equityContractId):
    bar_df = util.df(my_bars)
    bar_df['date'] = bar_df['date'].astype(pd.Timestamp)

    if symbol == "VXX":
        bar_df = bar_df.loc[lambda df: df.barCount > 0, :]
    else:
        bar_df = bar_df.loc[lambda df: df.volume > 0, :]
    bar_df['conId'] = conId
    bar_df['symbol'] = symbol
    bar_df['equityContractId'] = equityContractId
    bar_df['addedOn'] = datetime.datetime.now()
    return bar_df


def save_to_db(bars, conId):
    if not bars.empty:
        bars.to_sql("stock_3_min_bars", engine, if_exists="append", index=False,
                    dtype={'date': TIMESTAMP(timezone=True)})
    else:
        print("Data frame was empty.")
    result = connection.execute(equity_contracts.update().where(equity_contracts.c.conId == conId).values(
        threeMinuteBarsLoadedOn=datetime.datetime.now()))


def save_to_influx(bars, contract):
    if not bars.empty:
        bars = bars.set_index('date')
        return client.write(bars,
                            measurement='stock_3_min_bars',
                            symbol=contract.symbol,
                            contractId=str(contract.conId))


# skip_list =  ["SPY", "QQQ", "VXX", "EEM", "IWM", "XLF"]
skip_list = ["SPY", "QQQ", "EEM", "XLF", "GLD", "EFA", "IWM", "VXX", "FXI", "USO", "XOP", "HYG", "AAPL", "BAC", "MU",
             "FB", "BABA", "NVDA", "AMD", "GE", "TSLA", "NFLX",
             "AMZN", "MSFT", "SNAP", "T", "XOM", "DIS", "NXPI", "TWTR", "CSCO", "INTC", "JPM", "WMT", "C", "CHK", "BA",
             "WFC", "F", "AMAT", "JD", "ABBV", "SQ", "PBR", "BIDU", "CVX", "M", "GOOGL", "GM", "GS", "PYPL", "ADBE",
             "ATVI", "SHOP", "BRK B", "XLC", "XLY", "XLP", "XLE", "XLV", "XLI", "XLRE", "XLB", "XLK","JNK","VWD","IAU","AML",
             "SQQQ", "TQQQ","EWZ","GDX","XLU", "DIA","XBI","SPXL","TVIX","DWT","EWJ","IEMG","RSX","JNUG","SVXY","VWO","AMLP",
             "UVXY","DIA","XBI","AGG", "INDA", "EWW", "UGAZ","DGAZ","XRT","SPXS","EWY","VGK","UWT","EWC","IBB","SOXS","CAT","HD","CELG","VZ"]


def main():
    ib = connect_ib()

    query = 'select e.symbol, e."conId", e."equityContractId", min(b.date) as date, priority ' \
            'from equity_contracts e left join ' \
            'stock_3_min_bars b on e."equityContractId" = b."equityContractId" ' \
            'group by e.symbol, priority, e."equityContractId", e."conId" ' \
            'order by priority, e."equityContractId" '
    con_df = pd.read_sql(query, connection)
    tasks = []
    flag = True
    for index, row in con_df.iterrows():

        if row.symbol in skip_list:
            continue

        skip_list.append(row.symbol)

        my_con = Stock(conId=row.conId, exchange="SMART")
        q_con = ib.qualifyContracts(my_con)
        print(f"{datetime.datetime.now()} Processing contract {row.symbol}")

        if not pd.isnull(row.date):
            dt = row.date.astimezone(datetime.timezone.utc).strftime('%Y%m%d %H:%M:%S')
        else:
            dt = ''

        print(f"{datetime.datetime.now()} Processing contract {row.symbol} with end date { dt }")
        barsList = []
        fail_count = 0
        while True:
            start_time = time.time()
            try:
                bars = ib.reqHistoricalData(
                    my_con,
                    endDateTime=dt,
                    durationStr='20 D',
                    barSizeSetting='3 mins',
                    whatToShow='TRADES',
                    useRTH=False,
                    formatDate=2)

            except ValueError as e:
                print("Error getting historic bars for {} {}".format(row.symbol, e))
                exit(1)

            if not bars:
                if fail_count < 10:
                    dt = (datetime.datetime.strptime(dt, '%Y%m%d %H:%M:%S') - datetime.timedelta(minutes=5)).strftime(
                        '%Y%m%d %H:%M:%S')
                    fail_count += 1
                    print(f"Got no bars, trying 5 minutes earlier. Attempt {fail_count+1}")
                    continue
                elif fail_count >= 10 and fail_count < 20:
                    dt = (datetime.datetime.strptime(dt, '%Y%m%d %H:%M:%S') - datetime.timedelta(hours=1)).strftime(
                        '%Y%m%d %H:%M:%S')
                    fail_count += 1
                    print(f"Got no bars, trying 1 hour earlier. Attempt {fail_count+1}")
                    ib.sleep(5)
                    continue
                elif fail_count >= 20 and fail_count < 30:
                    dt = (datetime.datetime.strptime(dt, '%Y%m%d %H:%M:%S') - datetime.timedelta(days=1)).strftime(
                        '%Y%m%d %H:%M:%S')
                    fail_count += 1
                    print(f"Got no bars, trying 1 day earlier. Attempt {fail_count+1}")
                    ib.sleep(5)
                    continue
                print("No more bars")
                break

            dt = bars[0].date.strftime('%Y%m%d %H:%M:%S')
            print(dt)

            bar_df = to_df(bars, row.conId, row.symbol, row.equityContractId)
            print("Saving to DB")
            save_to_db(bar_df, row.conId)
            print("Saving to Influx")
            save_to_influx(bar_df, row)
            print("Execution time was: {}".format(str(time.time() - start_time)))


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

ib.run()
