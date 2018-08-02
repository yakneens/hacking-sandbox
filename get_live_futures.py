import asyncio
from ib_insync import *
import random
from aioinflux import InfluxDBClient
import os
import time

IB_PORT = os.environ.get('IB_PORT')

if not IB_PORT:
    IB_PORT = '4002'
#IB_PORT = '7496'
client = InfluxDBClient(db='stocks')


async def save_to_influx(bar, contract):
    new_bar = util.df([bar])
    new_bar = new_bar.set_index('time')
    await client.write(new_bar,
                       measurement='futures_5_sec_bars',
                       symbol=contract.symbol,
                       expiry=str(contract.lastTradeDateOrContractMonth.split(" ")[0]),
                       contractId=str(contract.conId),
                       local_symbol=contract.localSymbol)


def onError(reqId, errorCode, errorString, contract):
    print("ERROR", reqId, errorCode, errorString)


def onPendingTickers(tickers):
    for ticker in tickers:
        print(ticker)


def onBarUpdate(bars: BarDataList, hasNewBar: bool):
    if hasNewBar:
        new_bar = bars[-1]
        print(f'{str(new_bar.time)} {bars.contract.symbol}' )
        asyncio.get_event_loop().create_task(save_to_influx(new_bar, bars.contract))
        del bars[:]


new_client_id = int(random.random() * 100)

def connect_ib():
    print(f'Client ID {new_client_id}')
    ib = IB()
    ib.connect('127.0.0.1', IB_PORT, clientId=10)
    return ib

def main():

    ib = connect_ib()
    ib.errorEvent += onError
    ib.pendingTickersEvent += onPendingTickers
    ib.barUpdateEvent += onBarUpdate

    globex_instruments = ["ES", "NQ", "RTY", "NKD", "LE", "HE", "GF"]
    cont_fut = [ib.qualifyContracts(ContFuture(i, exchange="GLOBEX"))[0] for i in globex_instruments]

    ecbot_instruments = ["ZS", "ZL", "ZB", "ZF", "YM","ZM", "ZW", "ZC", "ZT","ZN","Z3N"]
    cont_fut += [ib.qualifyContracts(ContFuture(i, exchange="ECBOT"))[0] for i in ecbot_instruments]

    nymex_instruments = ["CL", "RB", "NG", "GC", "SI", "BZ", "HG"]
    cont_fut += [ib.qualifyContracts(ContFuture(i, exchange="NYMEX"))[0] for i in nymex_instruments]

    cfe_instruments = ["VIX"]
    cont_fut += [ib.qualifyContracts(ContFuture(i, exchange="CFE"))[0] for i in cfe_instruments]

    # cme_instruments = ["AUD", "GBP", "CAD", "JPY", "EUR", "GE"]
    # cont_fut += [ib.qualifyContracts(ContFuture(i, exchange="GLOBEX_CUR"))[0] for i in cme_instruments]

    fut = [ib.qualifyContracts(Future(conId=c.conId))[0] for c in cont_fut]

    for contract in fut:
        # ib.reqMktData(contract, '',False,False)
        ib.reqRealTimeBars(contract, 5, whatToShow="TRADES", useRTH=False)

    ib.run()

if __name__ == '__main__':
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