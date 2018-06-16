import datetime
from ib_insync import *
import pandas_market_calendars as mcal
import pathlib
import logging
import os
import time
import asyncio
from aioinflux import InfluxDBClient

def past_expiry(contract, my_date):
    return my_date.date() > datetime.datetime.strptime(contract.lastTradeDateOrContractMonth.split(" ")[0], '%Y%m%d').date()
    
def get_next_trading_day(contract, my_date):
    return my_date + datetime.timedelta(days=1)

def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("download_option_ticks.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'

    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    logging.basicConfig(filename=time.strftime("log/download_option_ticks.%y%m%d_%H%M%S.log"),
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

ib = IB()
ib.connect('127.0.0.1', 4002, clientId=5)
#"MSFT", "AMD", "SHOP", "BABA", "FB", "MU", "BAC", "NVDA", "AMZN", "AAPL", "VXX", "QQQ", "TSLA", "BIDU","GE","SNAP","T","C","CHK","WMT","JPM","INTC","CSCO","TWTR","NXPI","DIS","XOM","BA","WFC","F","AMAT","JD","ABBV","PBR","GOOGL","M","CVX","GS","GM","X","CAT","HD","CELG","VZ","IBM","JCP","PYPL","GILD","WLL","VRX","TEVA","FCX","CRM","MGM","SYMC","AAL","GOOG","PFE","ADBE","SPY" 
symbol_list = ["SQ", "NFLX", "CGC", "TSLA", "BIDU","GE","SNAP","T","C","CHK","WMT","JPM","INTC","CSCO","TWTR","NXPI","DIS","XOM","BA","WFC","F","AMAT","JD","ABBV"]
expiry_date = "20180615"
base_path = '/Users/siakhnin/Documents/trading/data/option_ticks/trades/'
#client = InfluxDBClient(mode='blocking', db='stocks')

for cur_symbol in symbol_list:
    general_contract = Option(symbol=cur_symbol, lastTradeDateOrContractMonth=expiry_date, exchange="SMART", multiplier="100", currency="USD")
    contract_details_list = ib.reqContractDetails(general_contract)
    print("Processing symbol {}".format(cur_symbol))
    logging.info("Processing symbol {}".format(cur_symbol))
    
    my_tick_dir = '{}{}/{}/'.format(base_path, cur_symbol, expiry_date)
    pathlib.Path(my_tick_dir).mkdir(parents=True, exist_ok=True) 
    
    my_bar_dir = '{}{}/{}/bars/'.format(base_path, cur_symbol, expiry_date)
    pathlib.Path(my_bar_dir).mkdir(parents=True, exist_ok=True) 
    
    for contract_details in contract_details_list:
        this_contract = contract_details.contract
        
        bar_file = pathlib.Path('{}{}_8hr_bars.csv'.format(my_bar_dir, this_contract.localSymbol.replace(" ", "_")))
        
        if not bar_file.exists():
        
            print("Contract {}".format(this_contract.localSymbol))
            
            try:
                first_date = ib.reqHeadTimeStamp(this_contract, "TRADES", False,2)
            except ValueError:
                logging.error("Error getting date for {}".format(this_contract.localSymbol))
                continue
            
            if not isinstance(first_date, datetime.datetime):
                logging.error("Bad date {}".format(first_date))
                continue
            
            print("First trading date: " + str(first_date))
            logging.info("First trading date: " + str(first_date))
    
            num_days = (datetime.datetime.now(datetime.timezone.utc) - first_date).days + 1
    
            print("Getting bars for {}: ".format(this_contract))
            logging.info("Getting bars for {}: ".format(this_contract))
            
            try:
                my_bars = ib.reqHistoricalData(this_contract, endDateTime='', durationStr='{} D'.format(num_days), barSizeSetting='8 hours', whatToShow='TRADES', useRTH=False, formatDate=2)
            except ValueError:
                logging.error("Error getting historic bars for {}".format(this_contract.localSymbol))
                continue
            
            if my_bars:           
                bar_df = util.df(my_bars)
                
                
                bar_df.to_csv(str(bar_file))
                
                start_date = first_date.replace(hour=0,minute=0,second=0)
                
                tickList = []
                last_date = ''
                
                for bar_data in my_bars:
                    cur_date = bar_data.date.replace(hour=0,minute=0,second=0)
                        
                    if bar_data.volume > 0 and cur_date != last_date:
                        last_date = cur_date
                        
                        print("Processing date: " + str(cur_date))
                        logging.info("Processing date: " + str(cur_date))
                        
                        while True:
                            ticks = ib.reqHistoricalTicks(this_contract, cur_date, None, 1000, 'TRADES', useRth=False)
                            tickList.append(ticks)
                            
                            if len(ticks) >= 1000:
                                cur_date = ticks[-1].time + datetime.timedelta(seconds=1)
                            else:
                                break
                            
                    else:
                        continue
                
                allTicks = [t for ticks in tickList for t in ticks]
                df = util.df(allTicks)
                df = df.set_index('time')
                
                #client.write(df, 
#                              measurement='option_trades', 
#                              symbol=this_contract.symbol,
#                              expiry=str(this_contract.lastTradeDateOrContractMonth.split(" ")[0]), 
#                              contractId=str(this_contract.conId),
#                              strike=str(this_contract.strike),
#                              right=this_contract.right,
#                              local_symbol=this_contract.localSymbol)
                
                filename = this_contract.localSymbol.replace(" ", "_")
                
                print("Writing to file {}: ".format(filename))
                logging.info("Writing to file {}: ".format(filename))
                
                df.to_csv('{}{}_trades.csv'.format(my_tick_dir, filename))
            else:
                print("No bars received for {}".format(this_contract.localSymbol))
                logging.info("No bars received for {}".format(this_contract.localSymbol))
                continue
        else:
            print("Bar file {} exists. Skipping".format(str(bar_file)))
            logging.info("Bar file {} exists. Skipping".format(str(bar_file)))
