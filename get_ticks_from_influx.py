import pandas as pd
from aioinflux import InfluxDBClient
import matplotlib.pyplot as plt

client = InfluxDBClient(mode='blocking', db='stocks',output='dataframe')
resp = client.query("select * from option_trades where contractId='311216696'")
ax = resp.plot(y='price')
plt.show()
resp.plot(kind='bar',y='size')
plt.show()