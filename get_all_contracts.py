import datetime
from ib_insync import *
import os
import time
import asyncio
from sqlalchemy import create_engine
import psycopg2 as pg
import io
import pandas as pd

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()
ib = IB()
ib.connect('127.0.0.1', 4002, clientId=1)
#"SPY","QQQ","EEM","IWM","XLF","GLD","EFA","VXX","USO","XOP","FXI","HYG","AAPL","BAC","MU","FB","BABA","NVDA","GE","AMD","TSLA","NFLX","AMZN","MSFT","SNAP","T","C","CHK","WMT","JPM","INTC","CSCO","TWTR","NXPI","DIS","XOM","BA","WFC","F",
#"AMAT","JD","ABBV","SQ","PBR","BIDU","GOOGL","M","CVX","GS","GM","X","CAT","HD","CELG","VZ","IBM","JCP","PYPL","GILD","WLL",
#"VRX","TEVA","FCX","CRM","MGM","SYMC","AAL","GOOG","PFE","ADBE","ROKU","RIG","SLB","QCOM","HMNY","MRK","CLF","V","KO","MA","CVS","DAL","JNJ","TGT","COST","UPS","CMCSA","LOW","SBUX","ORCL","WDC","DE","MS","BP","HAL","MCD","AKS","DVN","ETP","MRO","ATVI","BKNG","WYNN","CTL","BBY","LUV","EA","BMY","DB","WHR","PG","AVGO","CMG","NKE","KKR","GME","AMGN","APC","HTZ","BOX","DISH",
#"NBR","IQ","DWDP","KMI","CIT","LULU","UAA","KR","MDLZ","BB","SN","PEP","UNH","UTX","WFT","COP","MTCH","LB","ADSK","GPS","TWX","UAL","VLO","APA","FIT","LRCX","STX","AIG","HPQ","SWN","NTES","KHC","LMT","HLF","FTR","PM","WB","MYL","MO","FEYE","SPOT","OXY","AGN","ABX","DDD",
#"YELP","VALE","UNP","JNPR","AA","PANW","LVS","KSS","MDT","AXP","GPRO","MMM","OLED","EBAY","DLTR","AABA","EOG","BUD","FSLR","SPLK","P","MOMO","MPC","DKS","TTWO","LNG","CIEN","TXN","TRIP","YNDX","ILG","RTN","THC","CY","SWKS","SHOP","MT","HIG","TMUS","WBA","DHI","RRC","NE","AAOI","CZR","STI","WMB","CPB","TWLO","BZUN","FDX","MET","GERN","ABT","NYLD","OSTK","MDXG","VIPS","ZNGA","NEM","NWL","GIS","AFL","REGN","GLW","BWP","MRVL","NTNX","ESRX","NTAP",
symbol_list = ["ANF","BBT","STZ","CSX","SVU","AZN","NOC","SDRL","SHAK","CBS","KORS","MNST","VMW","HES","SYF","TROX","IP","BX","DNR","FL","URBN","ADM","I","AMTD","PAGS","TAL","TIF","HON","CTRP","GG","COTY","ETE","SRPT","W","EVHC","DBX","HPE","GRPN","ALXN","RHT","ULTA","AMBA","YY","BBBY","LBTYA","SO","Z","IGT","CRC","LLY","FOSL","USB","BIIB","OPK","CL","URI","NRG","PTLA","SCHW","NOK","PAY","PXD","TOL","K","ALGN","ESV","PSX","CWH","ARNC","ETFC","CMI","AZO","MNK"]
#"BRKB","MON","RDSA",
#symbol_list = ["NQ"]
#"SPX","RUT","MSCI","FTSE","NDX"
#SPX - RUT - MSCI - FTSE NDX
#"ES", "NQ-20", "GE", "OZ","LO", "OG" 
#symbol_list = ["AZO","MNK"]
#sema = asyncio.Semaphore(10)
def get_contracts(cur_symbol):
    general_contract = Option(symbol=cur_symbol, lastTradeDateOrContractMonth="", exchange="SMART", multiplier="100", currency="USD")

    print("getting {}".format(cur_symbol))
    return util.df([c.contract for c in ib.reqContractDetails(general_contract)])
    print("got {}".format(cur_symbol))

def main():
    #tasks = [asyncio.ensure_future(get_contracts(symbol)) for symbol in symbol_list]
    
    for symbol in symbol_list:
        result_df = get_contracts(symbol)
        result_df.to_sql("futures_opt_contracts", engine, if_exists="append")
    
    
    
    

if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Execution time was: {}".format(str(time.time() - start_time)))