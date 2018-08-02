import datetime
import time
import pandas as pd
from aioinflux import InfluxDBClient
from ib_insync import *
from sqlalchemy import create_engine, MetaData, update, TIMESTAMP
import logging
import os
import asyncio

import os

IB_PORT = os.environ.get('IB_PORT')

if not IB_PORT:
    IB_PORT = '4002'

from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()
meta = MetaData()
meta.reflect(bind=engine)

contract_daily_bars = meta.tables["contract_daily_bars"]
contract_table = meta.tables["contracts"]

Session = sessionmaker(bind=engine)
session = Session()

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
    if errorCode == 1102:
        logging.info("Restarting after outage")
        print("Restarting after outage")


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("download_option_ticks.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'

    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    logging.basicConfig(filename=time.strftime("log/get_option_ticks.%y%m%d.log"),
                        filemode="w",
                        level=logging.DEBUG,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)

SetupLogger()
logging.debug("now is %s", datetime.datetime.now())
logging.getLogger().setLevel(logging.INFO)

def connect_ib():
    ib = IB()
    ib.errorEvent += onError
    ib.RequestTimeout = 300
    ib.connect('127.0.0.1', IB_PORT, clientId=116, timeout=5)
    return ib

client = InfluxDBClient(mode='blocking', db='stocks')



query_template = 'select c.*, b."dailyBarId", b."date", b.volume from contracts c ' \
                 'join contract_daily_bars b on c."conId" = b."conId" ' \
                 'where {} and b.ticks_retrieved IS NULL and c.expired is not TRUE and ' \
                 'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now()) < {}  and ' \
                 'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  >= -3 ' \
                 'order by b.volume desc, c."lastTradeDateOrContractMonth", c.priority;'

days_to_expiry_cutoff = '1500'

priorities = [(1,' b.volume > 1000 '),
              (2, ' b.volume <= 1000 and b.volume > 500 '),
              (3, ' b.volume <= 500 and b.volume > 100 '),
              (4, ' b.volume <= 100 and b.volume >= 10'),
              (5, ' b.volume < 10 and b.volume >= 5') ]

def to_df(my_ticks, conId, symbol):
    tick_df = util.df(my_ticks)
    tick_df['time'] = tick_df['time'].astype(pd.Timestamp)
    tick_df['conId'] = conId
    tick_df['addedOn'] = datetime.datetime.now()
    return tick_df


def save_to_db(ticks, conId):
    if not ticks.empty:
        ticks.to_sql("option_ticks", engine, if_exists="append", index=False, dtype={'date': TIMESTAMP(timezone=True)})
    else:
        print("Data frame was empty")


def update_ticks_retrieved(this_bar):
    stmt = update(contract_daily_bars).where(contract_daily_bars.c.dailyBarId == this_bar.dailyBarId). \
        values(ticks_retrieved=True)
    connection.execute(stmt)

def main():
    ib = connect_ib()

    while True:
        start_time = time.time()

        for (priority_number, priority_sub) in priorities:
            if start_time - time.time() > 1800:
                print("Restarting")
                break

            print(f"Processing priority {priority_number}")
            logging.info(f"Processing priority {priority_number}")
            query = query_template.format(priority_sub, days_to_expiry_cutoff)

            con_df = pd.read_sql(query, connection)
            num_rows = len(con_df)
            flag = 0
            for index,row in con_df.iterrows():

                print(f"Processing contract {index}/{num_rows} {row.localSymbol} for {row.date} with volume {row.volume}")
                logging.info(f"Processing contract {row.localSymbol} for {row.date} with volume {row.volume}")
                cur_date = row.date.replace(hour=0, minute=0, second=0)
                if isinstance(cur_date, pd.Timestamp):
                    cur_date = cur_date.to_pydatetime()
                tickList = []

                if row.secType == "OPT":
                    this_contract = Option(conId=row.conId, exchange=row.exchange)
                elif row.secType == "FOP":
                    this_contract = FuturesOption(conId=row.conId, exchange=row.exchange)
                else:
                    print(f"Unknown security type {row.secType}")
                    exit(1)

                while True:
                    try:
                        ticks = ib.reqHistoricalTicks(this_contract, cur_date, None, 1000, 'TRADES', useRth=False)
                    except:
                        print("Couldn't get ticks")
                        raise
                    tickList.append(ticks)

                    if len(ticks) >= 1000:
                        cur_date = ticks[-1].time
                    else:
                        break

                if len(tickList) > 0:
                    allTicks = [t for ticks in tickList for t in ticks]
                    if allTicks:
                        df = to_df(allTicks, row.conId, row.symbol)

                        if not df.empty:
                            #df = df.set_index('time')
                            #df['addedOn'] = datetime.datetime.now()
                            #df['time'] = df['time'].astype(pd.Timestamp)
                            # print("Writing to Influx")
                            # logging.info("Writing to Influx")
                            #client.write(df,
                                         # measurement='option_trades',
                                         # symbol=row.symbol,
                                         # expiry=str(row.lastTradeDateOrContractMonth.split(" ")[0]),
                                         # contractId=str(row.conId),
                                         # strike=str(row.strike),
                                         # right=row.right,
                                         # local_symbol=row.localSymbol)
                            logging.info("Writing to DB")
                            print("Writing to DB")
                            save_to_db(df, row.conId)
                            update_ticks_retrieved(row)
                        else:
                            print("No ticks found")
                    else:
                        print("Allticks was empty")
                        update_ticks_retrieved(row)

                else:
                    logging.info("No ticks found")
                    print("No ticks found")


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