# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 15:06:24 2026

@author: Harmi
"""
# Test in Spyder first:
import yfinance as yf
df = yf.download("^NSEI", period="3y", interval="90m", progress=False)
print(df.shape)
print(df.head())