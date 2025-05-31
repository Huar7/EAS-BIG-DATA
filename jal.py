from typing import Dict

import yfinance as yf

from get_trending import trending


def main():
    tren = trending()
    nil = yf.download(tren, period="5y", interval="1wk")
    print(nil)

if __name__ == "__main__":
    main()
