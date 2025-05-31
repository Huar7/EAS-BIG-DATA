import yfinance as yf

spy = yf.Ticker('SPY').funds_data
print("desc: ", spy.description)
print("top holdings", spy.close)
