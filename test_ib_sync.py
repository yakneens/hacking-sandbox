import datetime
from ib_insync import *
import pandas_market_calendars as mcal


ib = IB()
ib.connect('127.0.0.1', 4002, clientId=1)

contract = Stock('TSLA', 'SMART', 'USD')


my_contract = FuturesOption(symbol='ES', lastTradeDateOrContractMonth='20180608', strike='2775', right='C', exchange='GLOBEX', multiplier='50', currency='USD')
my_contract_details = ib.reqContractDetails(my_contract)


es = Future('ES', '20180615', 'GLOBEX')
ib.qualifyContracts(es)
[ticker] = ib.reqTickers(es)
esValue  = ticker.marketPrice()
chains = ib.reqSecDefOptParams(es.symbol, 'GLOBEX', es.secType, es.conId)
util.df(chains)
chain = next(c for c in chains if c.tradingClass == 'ES' and c.exchange == 'GLOBEX')
strikes = [strike for strike in chain.strikes if esValue - 300 < strike < esValue + 300]
expirations = sorted(exp for exp in chain.expirations)
rights = ['P', 'C']
contracts = [FuturesOption('ES', expiration, strike, right, 'GLOBEX')
for right in rights
for expiration in expirations
for strike in strikes]

contracts = [FuturesOption('ES', '20180608', strike, right, 'GLOBEX')
for right in rights
for strike in strikes]

ib.qualifyContracts(*contracts)
len(contracts)

def past_expiry(contract, my_date):
        return my_date.date() > datetime.datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').date()
    
def get_next_trading_day(contract, my_date):
    return my_date + datetime.timedelta(days=1)
   

for my_contract in contracts:

    first_date = ib.reqHeadTimeStamp(my_contract, "TRADES", False,2)
    print("First trading date: " + str(first_date))
    
    
    # bars  =  ib.reqHistoricalData(
    #         es,
    #         endDateTime='',
    #         durationStr='60 D',
    #         barSizeSetting='8 hours',
    #         whatToShow='TRADES',
    #         useRTH=False,
    #         formatDate=1)
    
    #es = FuturesOption(symbol='ES', lastTradeDateOrContractMonth='20180629', strike='2775', right='C', exchange='GLOBEX', multiplier='50', currency='USD')
    #first_date = ib.reqHeadTimeStamp(es, "TRADES", False,2)
     
    start_date = first_date.replace(hour=0,minute=0,second=0)
    tickList = []
    while True:
        print("Processing date: " + str(start_date))
        
        if not past_expiry(my_contract,start_date):
            
            ticks = ib.reqHistoricalTicks(my_contract, start_date, None, 1000, 'TRADES', useRth=False)
            tickList.append(ticks)
            
            if len(ticks) >= 1000:
                start_date = ticks[-1].time + datetime.timedelta(seconds=1)
            else:
                start_date = get_next_trading_day(contract, start_date) 
            
        else:
            print("Done")
            break
        
     
    allTicks = [t for ticks in tickList for t in ticks]
    df = util.df(allTicks)
    filename = my_contract.localSymbol.replace(" ", "_")
    df.to_csv('/Users/siakhnin/Documents/trading/data/option_ticks/trades/{}_trades.csv'.format(filename))


    
#es_details = ContractDetails(es)
#start = ''
#end = datetime.datetime.now()
#ticks = ib.reqHistoricalTicks(es, start, end, 1000, 'TRADES', useRth=False)
#print(ticks)
