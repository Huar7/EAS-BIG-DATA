import requests
import bs4
from bs4 import BeautifulSoup


def parse(source):
    soup = BeautifulSoup(source.content, "html.parser")

    ticker = []
    print(soup)
    for tbody in soup.find_all("tbody"):
        print("sukses 1")
        for tr in tbody.findAll("tr"):
            print("sukses 2")
            for tickerCol in tr.findAll("td"):
                # print(tickerCol)
                for span in tickerCol.findAll("span"):
                    print("Mie Sukses isi 2")
                    txt = span.get_text()
                    # Encoding in utf-8 to remove the u character from the list
                    ticker.append(txt.encode("utf-8"))
                    break
                break  # .. ini untuk melompati nilai bawahnya

    ticker = [t.decode("utf-8").strip() for t in ticker]
    return ticker


def trending():
    session = requests.Session()
    url = "https://finance.yahoo.com/trending-tickers"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    req = session.get(url, headers=headers)
    trend = parse(req)
    return trend


if __name__ == "__main__":
    trending()
