import yfinance as yf
import os
# Define the list of top 10 companies by market capitalization on NSE
companies = [
    {"name": "Reliance Industries Ltd.", "symbol": "RELIANCE.NS"},
    {"name": "Tata Consultancy Services Ltd.", "symbol": "TCS.NS"},
    {"name": "HDFC Bank Ltd.", "symbol": "HDFCBANK.NS"},
    {"name": "Hindustan Unilever Ltd.", "symbol": "HINDUNILVR.NS"},
    {"name": "Infosys Ltd.", "symbol": "INFY.NS"},
    {"name": "ICICI Bank Ltd.", "symbol": "ICICIBANK.NS"},
    {"name": "Bharti Airtel Ltd.", "symbol": "BHARTIARTL.NS"},
    {"name": "Kotak Mahindra Bank Ltd.", "symbol": "KOTAKBANK.NS"},
    {"name": "ITC Ltd.", "symbol": "ITC.NS"},
    {"name": "Housing Development Finance Corporation Ltd.", "symbol": "HDFC.NS"}
]

if __name__ == '__main__':
    for company in companies:
        symbol = company["symbol"]
        name = company["name"]
        # Fetch stock data using yfinance
        stock = yf.Ticker(symbol)
        current_price = stock.history(period='1d')["Close"].iloc[-1]
        # Format the stock price with 2 decimal places
        formatted_price = "{:.2f}".format(current_price)
        # Display the company name and stock price
        print(f"{name}: {formatted_price}")