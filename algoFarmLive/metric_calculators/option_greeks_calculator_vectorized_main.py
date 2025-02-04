import asyncio
import warnings
from datetime import datetime

import pandas as pd
import py_vollib_vectorized
from algoLibs.converters import TickMarketFeedColumns, EquityOptionGreeksColumns
from algoLibs.dao import InfluxDBClientManager
from algoLibs.data_handlers import InfluxDbDataHandler
from algoLibs.market_operations import EqOptionGreeksDataInfluxOperations
from algoLibs.securities import Option
from algoLibs.utils import CommonUtils, AppConstants

pd.set_option('display.max_colwidth', 1000)
pd.set_option('display.max_columns', None)

if __name__ == '__main__':
    measurement = "tick_market_feed"
    start_date_time = CommonUtils.LocalDT_to_InfluxDT('2024-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    end_date_time = CommonUtils.LocalDT_to_InfluxDT('2024-01-01 05:00:00', "%Y-%m-%d %H:%M:%S")
    influx_db_client_manager = InfluxDBClientManager()
    influx_db_data_handler = InfluxDbDataHandler(influx_db_client_manager)
    unique_symbols = influx_db_data_handler.get_derivative_tickers_for_daterange(influx_db_client_manager.bucket,
                                                                                 measurement,
                                                                                 start_date_time,
                                                                                 end_date_time,
                                                                                 ["NIFTY", "FINNIFTY", "BANKNIFTY"])

    nifty_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurement, AppConstants.NIFTY_50_SYMBOL,
                                                            start_date_time, end_date_time, True,
                                                            ["_time", TickMarketFeedColumns.tag, TickMarketFeedColumns.last_traded_price])

    bank_nifty_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurement,
                                                                 AppConstants.BANKNIFTY_SYMBOL, start_date_time,
                                                                 end_date_time, True,
                                                                 ["_time", TickMarketFeedColumns.tag, TickMarketFeedColumns.last_traded_price])

    fin_nifty_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurement,
                                                                AppConstants.FINNIFTY_SYMBOL, start_date_time,
                                                                end_date_time, True,
                                                                ["_time", TickMarketFeedColumns.tag, TickMarketFeedColumns.last_traded_price])

    for symbol in unique_symbols:
        error_count = 0
        if symbol.startswith(AppConstants.BANKNIFTY):
            underlying_data = bank_nifty_data
            name = AppConstants.BANKNIFTY
        elif symbol.startswith(AppConstants.FINNIFTY):
            underlying_data = fin_nifty_data
            name = AppConstants.FINNIFTY
        elif symbol.startswith(AppConstants.NIFTY):
            underlying_data = nifty_data
            name = AppConstants.NIFTY

        derivative = Option(name, symbol)
        maturity_date_str = derivative.expiry
        maturity_date = datetime.strptime(maturity_date_str, "%d%b%y")
        maturity_date = pd.Timestamp(maturity_date)
        equity_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurement, symbol,
                                                                 start_date_time, end_date_time, True,
                                                                 ["_time", TickMarketFeedColumns.tag, TickMarketFeedColumns.last_traded_price])
        equity_data = pd.merge(equity_data, underlying_data, on='_time', how='left')
        equity_data.rename(columns={TickMarketFeedColumns.last_traded_price + '_y': 'underlying_data_ltp',
                                    TickMarketFeedColumns.last_traded_price + '_x': TickMarketFeedColumns.last_traded_price}, inplace=True)
        equity_data = equity_data.dropna()
        equity_data['_time_date'] = equity_data['_time'].apply(lambda x: x.date())
        equity_data['_time_date'] = pd.to_datetime(equity_data['_time_date'])
        equity_data['days_to_expiry'] = (maturity_date - equity_data['_time_date'])
        equity_data['time_to_expiry'] = equity_data['days_to_expiry'].apply(lambda x: x.days / 365.0)
        equity_data = equity_data.reset_index(drop=True)

        if derivative.option_type == "Call":
            flag = "c"
        else:
            flag = "p"

        option_df = pd.DataFrame({'Flag': flag, 'S': equity_data['underlying_data_ltp'] / 100.0, 'K': int(derivative.strike),
                                  'T': equity_data['time_to_expiry'], 'R': 0.05, 'Price': equity_data['last_traded_price'] / 100.0})
        option_df['Flag'] = flag
        option_df['S'] = equity_data['underlying_data_ltp'] / 100.0
        option_df['K'] = int(derivative.strike)
        option_df['T'] = equity_data['time_to_expiry']
        option_df['R'] = 0.05
        option_df['Price'] = equity_data['last_traded_price'] / 100.0
        option_df['time'] = equity_data['_time']
        option_df = option_df.dropna()
        option_df = option_df.reset_index(drop=True)

        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always")  # To ensure all warnings are caught

            option_greek_data = py_vollib_vectorized.price_dataframe(option_df, flag_col='Flag',
                                                                     underlying_price_col='S', strike_col='K',
                                                                     annualized_tte_col='T', riskfree_rate_col='R',
                                                                     price_col='Price', model='black_scholes', inplace=False)

        with open('warnings_log.txt', 'a') as file:
            error_count += 1
            for warning in caught_warnings:
                warn_message = f"Warning: {warning.message} in {warning.filename}, line {warning.lineno}" \
                               f" - Error encountered for token: {symbol}, error_count: {error_count}\n"
                file.write(warn_message)

        option_greek_data = option_greek_data.dropna()
        option_greek_data = option_greek_data.reset_index(drop=True)

        equity_analytics_data = pd.DataFrame({EquityOptionGreeksColumns.tag: symbol,
                                              EquityOptionGreeksColumns.time: equity_data['_time'],
                                              EquityOptionGreeksColumns.last_traded_price: equity_data['last_traded_price'] / 100.0,
                                              EquityOptionGreeksColumns.underlyingPrice: equity_data['underlying_data_ltp'] / 100.0,
                                              EquityOptionGreeksColumns.impliedVolatility: option_greek_data['IV'],
                                              EquityOptionGreeksColumns.delta: option_greek_data['delta'],
                                              EquityOptionGreeksColumns.gamma: option_greek_data['gamma'],
                                              EquityOptionGreeksColumns.theta: option_greek_data['theta'],
                                              EquityOptionGreeksColumns.vega: option_greek_data['vega'],
                                              EquityOptionGreeksColumns.rho: option_greek_data['rho']})

        eq_option_greeks_data_influx_operations = EqOptionGreeksDataInfluxOperations()
        points = eq_option_greeks_data_influx_operations.extract_features(equity_analytics_data)
        asyncio.run(influx_db_client_manager.write_data_async(points))
        final_equity_data_list = []
