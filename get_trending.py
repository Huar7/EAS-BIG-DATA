import requests
import bs4
from bs4 import BeautifulSoup


def parse(source):
    soup = BeautifulSoup(source.content, 'html.parser')

    ticker = []
    print(soup)
    for tbody in soup.find_all('tbody'):
        print(tbody)
        for tr in tbody.findAll('tr'):
            print(tr)
            for tickerCol in tr.findAll('td'):
                print(tickerCol)
                for span in tickerCol.findAll('span', class_='symbol yf-5ogvqh'):
                    txt = span.get_text()
                # Encoding in utf-8 to remove the u character from the list
                    ticker.append(txt.encode('utf-8'))

    print(ticker)


def main():
    session = requests.Session()
    url = 'https://finance.yahoo.com/trending-tickers'
    req = session.get(url)
    parse(req)


if __name__ == '__main__':
    main()
