import datetime
import time
import asyncio
import pandas as pd
from ib_insync import *
from sqlalchemy import create_engine, update, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData
import random
import os

IB_PORT = os.environ.get('IB_PORT')

if not IB_PORT:
    IB_PORT = '4002'
# IB_PORT = '7496'
engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contracts = meta.tables["contracts"]

timeout_retry_flag = 0


def set_timeout_flag(flag_value: bool, conId):
    stmt = update(contracts). \
        where(contracts.c.conId == conId). \
        values(timestampReqTimedout=flag_value, timestampLoadAttemptDate=datetime.datetime.now(datetime.timezone.utc))

    session.execute(stmt)
    session.commit()


def onError(reqId, errorCode, errorString, contract):
    # print("ERROR", reqId, errorCode, errorString)
    if errorCode == 200 and errorString == 'No security definition has been found for the request':
        this_contract = session.query(contracts).filter_by(conId=contract.conId).first()
        if this_contract:
            expiry = datetime.datetime.strptime(this_contract.lastTradeDateOrContractMonth.split(" ")[0], '%Y%m%d')
            if expiry < datetime.datetime.now():
                print("Contract expired, setting expiry flag")
                stmt = update(contracts).where(contracts.c.conId == contract.conId).values(expired=True)
                session.execute(stmt)
                session.commit()
    if errorCode == 162:
        global timeout_retry_flag
        if timeout_retry_flag >= 5:
            print("Request timed out. Setting flag.")
            set_timeout_flag(True, contract.conId)
            timeout_retry_flag = 0
        else:
            timeout_retry_flag += 1
            print(f"Timeout try {timeout_retry_flag}")

    elif errorCode == 1102:
        print("Restarting after outage")

    print("Caught error")


def connect_ib():
    ib = IB()
    ib.RequestTimeout = 300
    ib.errorEvent += onError
    ib.connect('127.0.0.1', IB_PORT, clientId=int(random.random() * 1000))

    return ib

def get_timestamp(contract, ib):
    if contract.secType == "OPT":
        my_con = Option(conId=contract.conId, exchange=contract.exchange)
    elif contract.secType == "FOP":
        my_con = FuturesOption(conId=contract.conId, exchange=contract.exchange)
    else:
        print(f"Unknown security type {contract.secType}")
        exit(1)

    global timeout_retry_flag
    while True:
        try:
            first_date = ib.reqHeadTimeStamp(my_con, "TRADES", False, 2)
        except ValueError as e:
            if str(e) == "time data '-9223372036854775' does not match format '%Y%m%d  %H:%M:%S'":
                stmt = update(contracts). \
                    where(contracts.c.conId == contract.conId). \
                    values(cantGetFirstTimestamp=True,
                           timestampLoadAttemptDate=datetime.datetime.now(datetime.timezone.utc))

                session.execute(stmt)
                session.commit()
                set_timeout_flag(False, contract.conId)
                print("Can't get timestamp, setting flag.")
            raise e

        if not isinstance(first_date, datetime.datetime):
            if timeout_retry_flag == 0 or timeout_retry_flag >= 5:
                timeout_retry_flag = 0
                raise ValueError
            ib.sleep(10)

        else:
            timeout_retry_flag = 0
            break

    print(f"Completed {contract.conId} {str(first_date)}")

    return (contract.conId, first_date)


def main():
    ib = connect_ib()
    start_cutoff = 25
    end_cutoff = 120
    last_load = 4
    cant_get_timestamp = "true"
    date_order = "ASC"

    date_query = 'SELECT distinct c."lastTradeDateOrContractMonth"::date FROM contracts c ' \
                 'WHERE c."conId" NOT IN (SELECT DISTINCT "contractId" ' \
                 'FROM contract_ib_first_timestamp ' \
                 'WHERE "contractId" IS NOT NULL ' \
                 'AND "firstTimestamp" IS NOT NULL) ' \
                 'AND c."timestampReqTimedout" is not TRUE ' \
                 'AND expired IS NOT TRUE AND "cantGetFirstTimestamp" IS {} and ' \
                 'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  >= {} and ' \
                 'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  <= {} and ' \
                 '(c."timestampLoadAttemptDate" is null or ' \
                 'DATE_PART(\'day\', now() -  c."timestampLoadAttemptDate" :: timestamp with time zone) >= {}) ' \
                 'ORDER BY c."lastTradeDateOrContractMonth"::date {} '. \
        format(cant_get_timestamp,
               start_cutoff,
               end_cutoff,
               last_load,
               date_order)

    dates_df = pd.read_sql(date_query, connection)

    for index, row in dates_df.iterrows():

        query = 'SELECT c."conId", c."exchange", c."localSymbol", c."secType", c."lastTradeDateOrContractMonth", c."symbol" FROM contracts c ' \
                'WHERE c."conId" NOT IN (SELECT DISTINCT "contractId" ' \
                'FROM contract_ib_first_timestamp ' \
                'WHERE "contractId" IS NOT NULL ' \
                'AND "firstTimestamp" IS NOT NULL) ' \
                'AND c."timestampReqTimedout" is not TRUE ' \
                'AND expired IS NOT TRUE AND "cantGetFirstTimestamp" IS {} and ' \
                'c."lastTradeDateOrContractMonth" :: date = \'{}\' and ' \
                '(c."timestampLoadAttemptDate" is null or DATE_PART(\'day\', now() -  c."timestampLoadAttemptDate" :: timestamp with time zone) >= {}) ' \
                'ORDER BY c.priority ASC, c."timestampReqTimedout" '.format(cant_get_timestamp,
                                                   row.lastTradeDateOrContractMonth,
                                                   last_load)

        con_df = pd.read_sql(query, connection)
        num_rows = len(con_df)
        print(len(con_df))
        tasks = []
        flag = True
        for index, row in con_df.iterrows():
            if row.symbol == "AMGN":
                 flag = False
                 continue
            print(f'{index}/{num_rows} {row.localSymbol} {row.lastTradeDateOrContractMonth}')
            try:
                (conId, first_date) = get_timestamp(row, ib)
                result = connection.execute(
                    contract_timestamp_table.insert().values(contractId=row.conId, firstTimestamp=first_date,
                                                             addedOn=datetime.datetime.now()))
                set_timeout_flag(False, row.conId)
            except ValueError as e:
                continue


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
