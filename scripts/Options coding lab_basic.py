# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 15:06:24 2026

@author: Harmi
"""
from openchart import NSEData
import datetime

nse = NSEData()

end   = datetime.datetime(2024, 3, 15)
start = datetime.datetime(2024, 1, 1)

# First search to get the token and symbol details
results = nse.search('RELIANCE', 'EQ')
print(results)

# Get token and symbol from results
token  = results.iloc[2]['scripcode']   # RELIANCE-EQ row
symbol = results.iloc[2]['symbol']
print(f"Token: {token}, Symbol: {symbol}")

# Now fetch historical
data = nse.historical_direct(
    token       = token,
    symbol      = symbol,
    symbol_type = 'EQ',
    start       = start,
    end         = end,
    interval    = '1d'
)
print(data)