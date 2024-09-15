import asyncio
from datetime import datetime
import pandas as pd
from algoLibs.calculators import OptionGreeksCalculator
from algoLibs.converters import TickMarketFeedColumns, EquityOptionGreeksColumns
from algoLibs.dao import InfluxDBClientManager
from algoLibs.data_handlers import InfluxDbDataHandler
from algoLibs.market_operations import EqOptionGreeksDataInfluxOperations
from algoLibs.securities import Option
from algoLibs.utils import CommonUtils

from algoFarmLive.metric_calculators.option_greeks_calculator_vectorized_main import ApplicationConstants

if __name__ == '__main__':
    measurment = "tick_market_feed"
    start_date_time = CommonUtils.LocalDT_to_InfluxDT('2024-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    end_date_time = CommonUtils.LocalDT_to_InfluxDT('2024-01-01 05:00:00', "%Y-%m-%d %H:%M:%S")
    influx_db_client_manager = InfluxDBClientManager()
    influx_db_data_handler = InfluxDbDataHandler(influx_db_client_manager)
    unique_symbols = influx_db_data_handler.get_derivative_tickers_for_daterange(influx_db_client_manager.bucket,
                                                                                  measurment,
                                                                                  start_date_time,
                                                                                  end_date_time,
                                                                                  ["NIFTY", "FINNIFTY", "BANKNIFTY"])

    nifty_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurment,
                                                            ApplicationConstants.NIFTY_50_SYMBOL,
                                                            start_date_time,
                                                            end_date_time,
                                                            True,
                                                            ["_time", TickMarketFeedColumns.tag, TickMarketFeedColumns.last_traded_price])

    bank_nifty_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurment,
                                                                 ApplicationConstants.BANKNIFTY_SYMBOL,
                                                                 start_date_time,
                                                                 end_date_time,
                                                                 True,
                                                                 ["_time", TickMarketFeedColumns.tag, TickMarketFeedColumns.last_traded_price])

    fin_nifty_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurment,
                                                                ApplicationConstants.FINNIFTY_SYMBOL,
                                                                start_date_time,
                                                                end_date_time,
                                                                True,
                                                                ["_time", TickMarketFeedColumns.tag, TickMarketFeedColumns.last_traded_price])

    for symbol in unique_symbols:
        if symbol.startswith(ApplicationConstants.BANKNIFTY):
            underlying_data = bank_nifty_data
            name = ApplicationConstants.BANKNIFTY
        elif symbol.startswith(ApplicationConstants.FINNIFTY):
            underlying_data = fin_nifty_data
            name = ApplicationConstants.FINNIFTY
        elif symbol.startswith(ApplicationConstants.NIFTY):
            underlying_data = nifty_data
            name = ApplicationConstants.NIFTY

        derivative = Option(name, symbol)
        maturity_date_str = derivative.expiry
        maturity_date = datetime.strptime(maturity_date_str, "%d%b%y")
        maturity_date = pd.Timestamp(maturity_date)

        equity_data = influx_db_data_handler.get_data_for_ticker(influx_db_client_manager.bucket, measurment, symbol,
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

        implied_volatility = 0.0
        delta = 0.0
        gamma = 0.0
        theta = 0.0
        rho = 0.0
        put_call_parity = 0.0
        exercise_probability = 0.0
        prev_ltp = -1.0
        prev_underlying_ltp = -1.0

        for index, data_point in equity_data.iterrows():
            data_point_ts = data_point["_time"]
            underlying_data_points_before_ts = underlying_data[underlying_data["_time"] < data_point_ts]
            if underlying_data_points_before_ts.size > 0:
                underlying_data_point = underlying_data_points_before_ts.iloc[0]
                underlying_ltp = underlying_data_point["ltp"] / 100.0
                ltp = data_point["ltp"] / 100.0
                if (ltp == prev_ltp and round(underlying_ltp, 0) == round(prev_underlying_ltp, 0)):
                    prev_ltp = ltp
                    prev_underlying_ltp = underlying_ltp
                else:
                    implied_volatility_calculator = OptionGreeksCalculator(underlying_ltp, derivative.strike,
                                                                           derivative.expiry, underlying_data_point["_time"].date(), 0.05)
                    if derivative.option_type == "CE":
                        implied_volatility = implied_volatility_calculator.getImpliedVolatility(call_price=ltp)
                        option_greeks = implied_volatility_calculator.getOptionGreeks()
                        delta = option_greeks.callDelta
                        theta = option_greeks.callTheta
                        rho = option_greeks.callRho
                        gamma = option_greeks.callDelta2

                    elif derivative.option_type == "PE":
                        implied_volatility = implied_volatility_calculator.getImpliedVolatility(put_price=ltp)
                        option_greeks = implied_volatility_calculator.getOptionGreeks()
                        delta = option_greeks.putDelta
                        rho = option_greeks.putRho
                        theta = option_greeks.putTheta
                        gamma = option_greeks.callDelta2

                    data_point_list = [symbol, data_point["_time"], ltp, implied_volatility, delta, gamma, theta, rho]
                    final_equity_data_list.append(data_point_list)
                    prev_underlying_ltp = underlying_ltp
                    prev_ltp = ltp

        equity_analytics_data = pd.DataFrame(final_equity_data_list,
                                             columns=[EquityOptionGreeksColumns.tag, EquityOptionGreeksColumns.time,
                                                      EquityOptionGreeksColumns.last_traded_price, EquityOptionGreeksColumns.impliedVolatility,
                                                      EquityOptionGreeksColumns.delta, EquityOptionGreeksColumns.gamma,
                                                      EquityOptionGreeksColumns.theta, EquityOptionGreeksColumns.rho])

        eq_option_greeks_data_influx_operations = EqOptionGreeksDataInfluxOperations()
        points = eq_option_greeks_data_influx_operations.extract_features(equity_analytics_data)
        asyncio.run(influx_db_client_manager.write_data_async(points))
        final_equity_data_list = []


