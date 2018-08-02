import datetime
from ib_insync import *
import os
import time
import asyncio
from sqlalchemy import create_engine
import psycopg2 as pg
import io
import pandas as pd

import os

IB_PORT = os.environ.get('IB_PORT')

if not IB_PORT:
    IB_PORT = '4002'


def onError(reqId, errorCode, errorString, contract):
    print("ERROR", reqId, errorCode, errorString)
    if errorCode == 200 and errorString == 'No security definition has been found for the request':
        print("Bad contract")
    elif errorCode == 1102:
        print("Restarting after outage")
        # main()


engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()

def connect_ib():
    ib = IB()
    ib.RequestTimeout = 300
    ib.connect('127.0.0.1', IB_PORT, clientId=11)
    ib.errorEvent += onError
    return ib

priority_1_symbol_list = [("ES", "GLOBEX", "FOP"), ("NQ", "GLOBEX", "FOP"), ("SPX", "SMART", "OPT"), ("RUT", "SMART", "OPT"),("NDX", "SMART", "OPT"), ("SPY", "SMART", "OPT"),
               ("QQQ", "SMART", "OPT"), ("EEM", "SMART", "OPT"), ("IWM", "SMART", "OPT"), ("XLF", "SMART", "OPT"),
               ("GLD", "SMART", "OPT"),
               ("EFA", "SMART", "OPT"), ("VXX", "SMART", "OPT"), ("USO", "SMART", "OPT"), ("XOP", "SMART", "OPT"),
               ("FXI", "SMART", "OPT"),
               ("HYG", "SMART", "OPT"), ("XLC", "SMART", "OPT"), ("XLY", "SMART", "OPT"), ("XLP", "SMART", "OPT"),
               ("XLE", "SMART", "OPT"),
               ("XLV", "SMART", "OPT"), ("XLI", "SMART", "OPT"), ("XLRE", "SMART", "OPT"), ("XLB", "SMART", "OPT"),
               ("XLK", "SMART", "OPT"),
               ("XLU", "SMART", "OPT"), ("GDX", "SMART", "OPT"), ("UVXY", "SMART", "OPT"), ("EWZ", "SMART", "OPT"),
               ("TQQQ", "SMART", "OPT"),
               ("SQQQ", "SMART", "OPT"), ("AMLP", "SMART", "OPT"), ("IAU", "SMART", "OPT"), ("VWO", "SMART", "OPT"),
               ("JNK", "SMART", "OPT"),
               ("SVXY", "SMART", "OPT"), ("JNUG", "SMART", "OPT"), ("RSX", "SMART", "OPT"), ("IEMG", "SMART", "OPT"),
               ("EWJ", "SMART", "OPT"),
               ("SPXL", "SMART", "OPT"), ("XBI", "SMART", "OPT"), ("DIA", "SMART", "OPT"), ("SPXS", "SMART", "OPT"),
               ("XRT", "SMART", "OPT"),
               ("EWW", "SMART", "OPT"), ("INDA", "SMART", "OPT"), ("AGG", "SMART", "OPT"), ("VGK", "SMART", "OPT"),
               ("EWY", "SMART", "OPT"),
               ("EWC", "SMART", "OPT"), ("IBB", "SMART", "OPT"), ("SOXS", "SMART", "OPT"), ("AAPL", "SMART", "OPT"),
               ("BAC", "SMART", "OPT"),
               ("MU", "SMART", "OPT"), ("FB", "SMART", "OPT"), ("BABA", "SMART", "OPT"), ("NVDA", "SMART", "OPT"),
               ("GE", "SMART", "OPT"),
               ("AMD", "SMART", "OPT"), ("TSLA", "SMART", "OPT"), ("NFLX", "SMART", "OPT"), ("AMZN", "SMART", "OPT"),
               ("MSFT", "SMART", "OPT"),
               ("SNAP", "SMART", "OPT"), ("T", "SMART", "OPT"), ("C", "SMART", "OPT"), ("CHK", "SMART", "OPT"),
               ("WMT", "SMART", "OPT"), ("JPM", "SMART", "OPT"),
               ("INTC", "SMART", "OPT"), ("CSCO", "SMART", "OPT"), ("TWTR", "SMART", "OPT"), ("NXPI", "SMART", "OPT"),
               ("DIS", "SMART", "OPT"),
               ("XOM", "SMART", "OPT"), ("BA", "SMART", "OPT"), ("WFC", "SMART", "OPT"), ("F", "SMART", "OPT"),
               ("AMAT", "SMART", "OPT"), ("JD", "SMART", "OPT"),
               ("ABBV", "SMART", "OPT"), ("SQ", "SMART", "OPT"), ("PBR", "SMART", "OPT"), ("BIDU", "SMART", "OPT"),
               ("GOOGL", "SMART", "OPT"),
               ("M", "SMART", "OPT"), ("CVX", "SMART", "OPT"), ("GS", "SMART", "OPT"), ("GM", "SMART", "OPT"),
               ("X", "SMART", "OPT"), ("CAT", "SMART", "OPT"),
               ("HD", "SMART", "OPT"), ("CELG", "SMART", "OPT"), ("VZ", "SMART", "OPT"), ("IBM", "SMART", "OPT"),
               ("JCP", "SMART", "OPT"),
               ("PYPL", "SMART", "OPT"), ("GILD", "SMART", "OPT"), ("WLL", "SMART", "OPT"), ("BRK B", "SMART", "OPT"),
               ("TEVA", "SMART", "OPT"), ("FCX", "SMART", "OPT"), ("CRM", "SMART", "OPT"), ("MGM", "SMART", "OPT"),
               ("SYMC", "SMART", "OPT"),
               ("AAL", "SMART", "OPT"), ("GOOG", "SMART", "OPT"), ("PFE", "SMART", "OPT"), ("ADBE", "SMART", "OPT"),
               ("ROKU", "SMART", "OPT"),
               ("RIG", "SMART", "OPT"), ("SLB", "SMART", "OPT"), ("QCOM", "SMART", "OPT"),
               ("MRK", "SMART", "OPT"),
               ("CLF", "SMART", "OPT"), ("V", "SMART", "OPT"), ("KO", "SMART", "OPT"), ("MA", "SMART", "OPT"),
               ("CVS", "SMART", "OPT"), ("DAL", "SMART", "OPT"),
               ("JNJ", "SMART", "OPT"), ("TGT", "SMART", "OPT"), ("COST", "SMART", "OPT"), ("UPS", "SMART", "OPT"),
               ("CMCSA", "SMART", "OPT"),
               ("LOW", "SMART", "OPT"), ("SBUX", "SMART", "OPT"), ("ORCL", "SMART", "OPT"), ("WDC", "SMART", "OPT"),
               ("DE", "SMART", "OPT"),
               ("MS", "SMART", "OPT"), ("BP", "SMART", "OPT"), ("HAL", "SMART", "OPT"), ("MCD", "SMART", "OPT"),
               ("AKS", "SMART", "OPT"), ("DVN", "SMART", "OPT"),
               ("ETP", "SMART", "OPT"), ("MRO", "SMART", "OPT"), ("ATVI", "SMART", "OPT"), ("BKNG", "SMART", "OPT")]

priority_2_symbol_list = [("BZ", "NYMEX", "FOP"), ("AUD", "GLOBEX", "FOP"), ("GBP", "GLOBEX", "FOP"), ("CAD", "GLOBEX", "FOP"),
               ("JPY", "GLOBEX", "FOP"),
               ("EUR", "GLOBEX", "FOP"), ("SI", "NYMEX", "FOP"), ("GC", "NYMEX", "FOP"), ("RTY", "GLOBEX", "FOP"),
               ("RB", "NYMEX", "FOP"), ("HE", "GLOBEX", "FOP"), ("LE", "GLOBEX", "FOP"), ("ZM", "ECBOT", "FOP"),
               ("ZW", "ECBOT", "FOP"),
               ("ZS", "ECBOT", "FOP"), ("ZC", "ECBOT", "FOP"), ("NG", "NYMEX", "FOP"), ("ZT", "ECBOT", "FOP"),
               ("ZF", "ECBOT", "FOP"), ("ZN", "ECBOT", "FOP"), ("ZB", "ECBOT", "FOP"),
               ("CL", "NYMEX", "FOP"), ("YM", "ECBOT", "FOP"), ("GE", "GLOBEX", "FOP"),("MSCI", "SMART", "OPT"),]

priority_3_symbol_list = [("WYNN", "SMART", "OPT"),
               ("CTL", "SMART", "OPT"), ("BBY", "SMART", "OPT"), ("LUV", "SMART", "OPT"), ("EA", "SMART", "OPT"),
               ("BMY", "SMART", "OPT"), ("DB", "SMART", "OPT"),
               ("WHR", "SMART", "OPT"), ("PG", "SMART", "OPT"), ("AVGO", "SMART", "OPT"), ("CMG", "SMART", "OPT"),
               ("NKE", "SMART", "OPT"),
               ("KKR", "SMART", "OPT"), ("GME", "SMART", "OPT"), ("AMGN", "SMART", "OPT"), ("APC", "SMART", "OPT"),
               ("HTZ", "SMART", "OPT"),
               ("BOX", "SMART", "OPT"), ("DISH", "SMART", "OPT"), ("NBR", "SMART", "OPT"), ("IQ", "SMART", "OPT"),
               ("DWDP", "SMART", "OPT"),
               ("KMI", "SMART", "OPT"), ("CIT", "SMART", "OPT"), ("LULU", "SMART", "OPT"), ("UAA", "SMART", "OPT"),
               ("KR", "SMART", "OPT"),
               ("MDLZ", "SMART", "OPT"), ("BB", "SMART", "OPT"), ("SN", "SMART", "OPT"), ("PEP", "SMART", "OPT"),
               ("UNH", "SMART", "OPT"),
               ("UTX", "SMART", "OPT"), ("WFT", "SMART", "OPT"), ("COP", "SMART", "OPT"), ("MTCH", "SMART", "OPT"),
               ("LB", "SMART", "OPT"),
               ("ADSK", "SMART", "OPT"), ("GPS", "SMART", "OPT"), ("UAL", "SMART", "OPT"), ("VLO", "SMART", "OPT"),
               ("APA", "SMART", "OPT"),
               ("FIT", "SMART", "OPT"), ("LRCX", "SMART", "OPT"), ("STX", "SMART", "OPT"), ("AIG", "SMART", "OPT"),
               ("HPQ", "SMART", "OPT"),
               ("SWN", "SMART", "OPT"), ("NTES", "SMART", "OPT"), ("KHC", "SMART", "OPT"), ("LMT", "SMART", "OPT"),
               ("HLF", "SMART", "OPT"),
               ("FTR", "SMART", "OPT"), ("PM", "SMART", "OPT"), ("WB", "SMART", "OPT"), ("MYL", "SMART", "OPT"),
               ("MO", "SMART", "OPT"), ("FEYE", "SMART", "OPT"),
               ("SPOT", "SMART", "OPT"), ("OXY", "SMART", "OPT"), ("AGN", "SMART", "OPT"), ("ABX", "SMART", "OPT"),
               ("DDD", "SMART", "OPT"),
               ("YELP", "SMART", "OPT"), ("VALE", "SMART", "OPT"), ("UNP", "SMART", "OPT"), ("JNPR", "SMART", "OPT"),
               ("AA", "SMART", "OPT"),
               ("PANW", "SMART", "OPT"), ("LVS", "SMART", "OPT"), ("KSS", "SMART", "OPT"), ("MDT", "SMART", "OPT"),
               ("AXP", "SMART", "OPT"),
               ("GPRO", "SMART", "OPT"), ("MMM", "SMART", "OPT"), ("OLED", "SMART", "OPT"), ("EBAY", "SMART", "OPT"),
               ("DLTR", "SMART", "OPT"),
               ("AABA", "SMART", "OPT"), ("EOG", "SMART", "OPT"), ("BUD", "SMART", "OPT"), ("FSLR", "SMART", "OPT"),
               ("SPLK", "SMART", "OPT"),
               ("P", "SMART", "OPT"), ("MOMO", "SMART", "OPT"), ("MPC", "SMART", "OPT"), ("DKS", "SMART", "OPT"),
               ("TTWO", "SMART", "OPT"),
               ("LNG", "SMART", "OPT"), ("CIEN", "SMART", "OPT"), ("TXN", "SMART", "OPT"), ("TRIP", "SMART", "OPT"),
               ("YNDX", "SMART", "OPT"),
               ("ILG", "SMART", "OPT"), ("RTN", "SMART", "OPT"), ("THC", "SMART", "OPT"), ("CY", "SMART", "OPT"),
               ("SWKS", "SMART", "OPT"),
               ("SHOP", "SMART", "OPT"), ("MT", "SMART", "OPT"), ("HIG", "SMART", "OPT"), ("TMUS", "SMART", "OPT"),
               ("WBA", "SMART", "OPT"),
               ("DHI", "SMART", "OPT"), ("RRC", "SMART", "OPT"), ("NE", "SMART", "OPT"), ("AAOI", "SMART", "OPT"),
               ("CZR", "SMART", "OPT"),
               ("STI", "SMART", "OPT"), ("WMB", "SMART", "OPT"), ("CPB", "SMART", "OPT"), ("TWLO", "SMART", "OPT"),
               ("BZUN", "SMART", "OPT"),
               ("FDX", "SMART", "OPT"), ("MET", "SMART", "OPT"), ("GERN", "SMART", "OPT"), ("ABT", "SMART", "OPT"),
               ("NYLD", "SMART", "OPT"),
               ("OSTK", "SMART", "OPT"), ("MDXG", "SMART", "OPT"), ("VIPS", "SMART", "OPT"), ("ZNGA", "SMART", "OPT"),
               ("NEM", "SMART", "OPT"),
               ("NWL", "SMART", "OPT"), ("GIS", "SMART", "OPT"), ("AFL", "SMART", "OPT"), ("REGN", "SMART", "OPT"),
               ("GLW", "SMART", "OPT"),
               ("BWP", "SMART", "OPT"), ("MRVL", "SMART", "OPT"), ("NTNX", "SMART", "OPT"), ("ESRX", "SMART", "OPT"),
               ("NTAP", "SMART", "OPT"),
               ("ANF", "SMART", "OPT"), ("BBT", "SMART", "OPT"), ("STZ", "SMART", "OPT"), ("CSX", "SMART", "OPT"),
               ("SVU", "SMART", "OPT"),
               ("AZN", "SMART", "OPT"), ("NOC", "SMART", "OPT"), ("SHAK", "SMART", "OPT"), ("CBS", "SMART", "OPT"),
               ("KORS", "SMART", "OPT"),
               ("MNST", "SMART", "OPT"), ("VMW", "SMART", "OPT"), ("HES", "SMART", "OPT"), ("SYF", "SMART", "OPT"),
               ("TROX", "SMART", "OPT"),
               ("IP", "SMART", "OPT"), ("BX", "SMART", "OPT"), ("DNR", "SMART", "OPT"), ("FL", "SMART", "OPT"),
               ("URBN", "SMART", "OPT"), ("ADM", "SMART", "OPT"),
               ("I", "SMART", "OPT"), ("AMTD", "SMART", "OPT"), ("PAGS", "SMART", "OPT"), ("TAL", "SMART", "OPT"),
               ("TIF", "SMART", "OPT"),
               ("HON", "SMART", "OPT"), ("CTRP", "SMART", "OPT"), ("GG", "SMART", "OPT"), ("COTY", "SMART", "OPT"),
               ("ETE", "SMART", "OPT"),
               ("SRPT", "SMART", "OPT"), ("W", "SMART", "OPT"), ("EVHC", "SMART", "OPT"), ("DBX", "SMART", "OPT"),
               ("HPE", "SMART", "OPT"),
               ("GRPN", "SMART", "OPT"), ("ALXN", "SMART", "OPT"), ("RHT", "SMART", "OPT"), ("ULTA", "SMART", "OPT"),
               ("AMBA", "SMART", "OPT"),
               ("YY", "SMART", "OPT"), ("BBBY", "SMART", "OPT"), ("LBTYA", "SMART", "OPT"), ("SO", "SMART", "OPT"),
               ("Z", "SMART", "OPT"),
               ("IGT", "SMART", "OPT"), ("CRC", "SMART", "OPT"), ("LLY", "SMART", "OPT"), ("FOSL", "SMART", "OPT"),
               ("USB", "SMART", "OPT"),
               ("BIIB", "SMART", "OPT"), ("OPK", "SMART", "OPT"), ("CL", "SMART", "OPT"), ("URI", "SMART", "OPT"),
               ("NRG", "SMART", "OPT"),
               ("PTLA", "SMART", "OPT"), ("SCHW", "SMART", "OPT"), ("NOK", "SMART", "OPT"), ("PAY", "SMART", "OPT"),
               ("PXD", "SMART", "OPT"),
               ("TOL", "SMART", "OPT"), ("K", "SMART", "OPT"), ("ALGN", "SMART", "OPT"), ("ESV", "SMART", "OPT"),
               ("PSX", "SMART", "OPT"),
               ("CWH", "SMART", "OPT"), ("ARNC", "SMART", "OPT"), ("ETFC", "SMART", "OPT"), ("CMI", "SMART", "OPT"),
               ("AZO", "SMART", "OPT"),
               ("MNK", "SMART", "OPT")]

symbols = {1:priority_1_symbol_list, 2:priority_2_symbol_list, 3:priority_3_symbol_list}



# skip_list = ["BZ", "AUD","GBP", "CAD","JPY", "EUR", "SI", "GC","RTY","RB","HE","LE","ZM","ZW","ZS","NG","ZT","ZF","ZN","ZC","ZB", "CL","YM","GE","ES", "NQ", "NDX", "SPX", "SPY", "RUT", "MSCI", "QQQ", "EEM", "IWM", "XLF", "GLD", "EFA", "VXX", "USO",
# "XOP", "FXI", "HYG",
# "XLC", "XLY", "XLP",
# "XLE", "XLV", "XLI", "XLRE", "XLB", "XLK", "XLU", "GDX", "UVXY", "EWZ",
# "TQQQ", "SQQQ", "AMLP", "IAU", "VWO", "JNK", "SVXY", "JNUG", "RSX", "IEMG",
# "EWJ", "SPXL", "XBI", "DIA", "SPXS", "XRT", "EWW", "INDA", "AGG", "VGK",
# "EWY", "EWC", "IBB", "SOXS", "AAPL", "BAC", "MU",
# "FB", "BABA", "NVDA", "GE", "AMD", "TSLA", "NFLX", "AMZN", "MSFT", "SNAP", "T", "C", "CHK", "WMT", "JPM",
# "INTC", "CSCO", "TWTR", "NXPI", "DIS", "XOM", "BA", "WFC", "F", "AMAT", "JD", "ABBV", "SQ", "PBR",
# "BIDU", "GOOGL", "M", "CVX", "GS", "GM", "X", "CAT", "HD", "CELG",
# "VZ", "IBM", "JCP", "PYPL", "GILD", "WLL", "BRK B", "VRX", "TEVA", "FCX", "CRM", "MGM", "SYMC", "AAL",
# "GOOG", "PFE", "ADBE", "ROKU", "RIG", "SLB", "QCOM", "MRK", "CLF", "V", "KO", "MA", "CVS", "DAL",
# "JNJ", "TGT", "COST", "UPS", "CMCSA", "LOW", "SBUX", "ORCL", "WDC", "DE", "MS", "BP", "HAL", "MCD",
# "AKS", "DVN", "ETP", "MRO", "ATVI", "BKNG", "WYNN", "CTL", "BBY", "LUV", "EA", "BMY", "DB", "WHR", "PG",
# "AVGO",
# "CMG", "NKE", "KKR", "GME", "AMGN", "APC", "HTZ", "BOX", "DISH", "NBR", "IQ", "DWDP", "KMI", "CIT",
# "LULU", "UAA", "KR", "MDLZ", "BB", "SN", "PEP", "UNH", "UTX", "WFT",
# "COP", "MTCH", "LB", "ADSK", "GPS", "UAL", "VLO", "APA", "FIT", "LRCX", "STX", "AIG", "HPQ",
# "SWN", "NTES", "KHC", "LMT", "HLF", "FTR", "PM", "WB", "MYL", "MO", "FEYE", "SPOT", "OXY", "AGN", "ABX",
# "DDD", "YELP", "VALE", "UNP", "JNPR", "AA", "PANW", "LVS", "KSS", "MDT", "AXP", "GPRO", "MMM", "OLED",
# "EBAY", "DLTR", "AABA", "EOG", "BUD", "FSLR", "SPLK", "P", "MOMO", "MPC", "DKS", "TTWO", "LNG", "CIEN",
# "TXN", "TRIP", "YNDX", "ILG", "RTN", "THC", "CY", "SWKS", "SHOP", "MT", "HIG", "TMUS", "WBA", "DHI",
# "RRC",
# "NE", "AAOI", "CZR", "STI", "WMB", "CPB", "TWLO", "BZUN", "FDX", "MET", "GERN", "ABT", "NYLD", "OSTK",
# "MDXG", "VIPS", "ZNGA", "NEM", "NWL", "GIS", "AFL", "REGN", "GLW", "BWP", "MRVL", "NTNX", "ESRX", "NTAP",
# "ANF", "BBT", "STZ", "CSX", "SVU", "AZN", "NOC", "SHAK", "CBS", "KORS", "MNST", "VMW", "HES",
# "SYF", "TROX", "IP", "BX", "DNR", "FL", "URBN", "ADM", "I", "AMTD", "PAGS", "TAL", "TIF", "HON", "CTRP",
# "GG", "COTY", "ETE", "SRPT", "W", "EVHC", "DBX", "HPE", "GRPN", "ALXN", "RHT", "ULTA", "AMBA", "YY",
# "BBBY", "LBTYA", "SO", "Z", "IGT", "CRC", "LLY", "FOSL", "USB", "BIIB", "OPK", "CL", "URI", "NRG",
# "PTLA", "SCHW", "NOK", "PAY", "PXD", "TOL", "K", "ALGN", "ESV", "PSX", "CWH", "ARNC", "ETFC", "CMI",
# "AZO", "MNK"]
skip_list = []

#skip_list = []

def get_contracts(cur_symbol, cur_exchange, obj_type, ib):
    if obj_type == "OPT":
        general_contract = Option(symbol=cur_symbol, lastTradeDateOrContractMonth="", exchange=cur_exchange,
                                  currency="USD")
    elif obj_type == "FOP":
        general_contract = FuturesOption(symbol=cur_symbol, lastTradeDateOrContractMonth="", exchange=cur_exchange,
                                         currency="USD")
    else:
        print(f"Unknown object type {obj_type}")
        exit(1)
    print("getting {}".format(cur_symbol))

    return util.df([c.contract for c in ib.reqContractDetails(general_contract)])
    print("got {}".format(cur_symbol))


def main():
    ib = connect_ib()
    # tasks = [asyncio.ensure_future(get_contracts(symbol)) for symbol in symbol_list]
    query = 'select distinct "conId", symbol from contracts'
    con_df = pd.read_sql(query, connection)

    for priority in symbols:
        print(f"Processing priority {priority}")
        symbol_list = symbols[priority]

        for (symbol, exchange, obj_type) in symbol_list:

            if symbol in skip_list:
                continue

            skip_list.append(symbol)

            con_ids = con_df.query('symbol == @symbol')['conId']

            try:
                result_df = get_contracts(symbol, exchange, obj_type, ib)
            except ValueError as e:
                continue

            filtered_result_df = result_df.query('conId not in @con_ids')
            filtered_result_df['priority'] = priority
            print(f"Adding {len(filtered_result_df)} new contracts")
            filtered_result_df.to_sql("contracts", engine, if_exists="append")




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
