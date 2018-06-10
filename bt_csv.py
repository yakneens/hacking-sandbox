from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])
import matplotlib
# Import the backtrader platform
import backtrader as bt
import backtrader.analyzers as btanalyzers
import backtrader.feeds as btfeeds
import backtrader.strategies as btstrats
import pyfolio as pf

class MyCSV(btfeeds.GenericCSVData):
    linesoverride = True  # discard usual OHLC structure
    # datetime must be present and last
    lines = ('open', 'high', 'low','close','volume','wap','hasGaps','count','datetime')
    # datetime (always 1st) and then the desired order for
    params = (
        # (datetime, 0), # inherited from parent class
        ('open', 1),  # default field pos 1
        ('high', 2),  # default field pos 2
        ('low', 3),  # default field pos 2
        ('close', 4),  # default field pos 2
        ('volume', 5),  # default field pos 2
        ('wap', 6),  # default field pos 2
        ('hasGaps', 7),  # default field pos 2
        ('count', 8),  # default field pos 2
    )

# Create a Stratey
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),('printlog', True),('longmaperiod', 1000)
    )

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            ''' Logging function fot this strategy'''
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

        # To keep track of pending orders
        self.order = None
        self.buyprice = None
        self.buycomm = None
        
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod)
        
        self.sma_long = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.longmaperiod)
        # Indicators for the plotting show
        
        #bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        #bt.indicators.WeightedMovingAverage(self.datas[0], period=25).subplot = True
        #bt.indicators.StochasticSlow(self.datas[0])
        #bt.indicators.MACDHisto(self.datas[0])
        #rsi = bt.indicators.RSI(self.datas[0])
        #bt.indicators.SmoothedMovingAverage(rsi, period=10)
        #bt.indicators.ATR(self.datas[0]).plot = False

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None
        
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
    
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))
    def next(self):
        # Simply log the closing price of the series from the reference
        #self.log('Close, %.2f' % self.dataclose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            if self.sma[0] > self.sma_long[0]:
            #if self.dataclose[0] > self.sma[0]:

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()

        else:

            if self.sma[0] < self.sma_long[0]:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()
    def stop(self):
        self.log('(MA Period %2d) Ending Value %.2f' %
             (self.params.maperiod, self.broker.getvalue()), doprint=True)           

if __name__ == '__main__':
    # Create a cerebro entity
    
    cerebro = bt.Cerebro()

    
    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    datapath = os.path.join("","/Users/siakhnin/Documents/trading/data/QQQ_16_03_2018-16_05_2018_30_secs.csv")
    

    data = MyCSV(
        dataname=datapath,
        dtformat='%Y-%m-%d %H:%M:%S',
        timeframe=bt.TimeFrame.Seconds,
    )

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    
#     cerebro.resampledata(
#         data,
#         timeframe=bt.TimeFrame.Minutes,
#         compression=15,
#     )
    
        # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)

    # Set our desired cash start
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001) # 0.1% ... divide by 100 to remove the %
    cerebro.addanalyzer(btanalyzers.SharpeRatio, _name='mysharpe')
    #cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    
    # Add a strategy
    strats = cerebro.addstrategy(TestStrategy, maperiod=200)

    # Print out the starting conditions
    #print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    results = cerebro.run()
    my_strat = results[0]

    print('Sharpe Ratio:', my_strat.analyzers.mysharpe.get_analysis())
    
    #pyfoliozer = my_strat.analyzers.getbyname('pyfolio')
#     returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
#     pf.create_full_tear_sheet(
#     returns,
#     positions=positions,
#     transactions=transactions,
# #    gross_lev=gross_lev,
#     live_start_date='2000-01-01',  # This date is sample specific
#     round_trips=True)
    # Print out the final result
    #print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.plot()
    