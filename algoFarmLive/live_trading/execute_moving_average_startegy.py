import asyncio
from datetime import datetime, timedelta

import algoLibs as libs
from algoLibs import AngelBrokingBrokerage, NaiveAllocationStrategy, LivePortfolio, InfluxAggregationType, CommonUtils
from algoLibs.tenor import Calendar

from algoFarmLive.live_trading.startegies.moving_average_startegy import MovingAverageStrategy

if __name__ == '__main__':

    service_name = "moving_average_start_manager"
    bootstrap_servers = libs.PropertyManager.getValue(libs.AppConstants.BOOTSTRAP_SERVERS)
    moving_average_long_window = 50
    moving_average_short_window = 20
    moving_average_stategy =  MovingAverageStrategy('moving_average_crossover_20_50',moving_average_short_window,moving_average_long_window)
    tickers = ["AXISBANK-EQ", "APOLLOTYRE-EQ", "SBIN-EQ", "CIPLA-EQ", "POWERGRID-EQ"]
    influx_client_manager = libs.InfluxDBClientManager()
    redis_client_manager = libs.RedisClientManager()
    capital = 100000
    angelBroking = AngelBrokingBrokerage()
    allocation_strategy = NaiveAllocationStrategy(capital, 1000.0, len(tickers))
    live_portfolio = LivePortfolio(capital,angelBroking,allocation_strategy)
    calendar = Calendar('/Users/abhishekvijaykumar/PycharmProjects/AlgoFarmPlus/AlgoLibs/data/Holidays.csv')
    tenor_preceding = libs.Tenor(libs.RollRule.MODPREVIOUS, calendar)
    tenor_following = libs.Tenor(libs.RollRule.MODFOLLOW, calendar)
    # start_time = CommonUtils.get_market_open_utc_datetime() datetime(2024, 6, 3, 0, 0, 0)
    end_dt = CommonUtils.get_market_open_utc_datetime(datetime(2025, 1, 24, 0, 0, 0)) + timedelta(minutes=1)
    start_dt =  CommonUtils.get_market_close_utc_datetime(tenor_preceding.adjust_date(end_dt,"-1d")) - timedelta(minutes=moving_average_long_window-1)
    # start_dt =  CommonUtils.get_market_open_utc_datetime(datetime(2025, 1, 24, 3, 45, 0))
    equityMarketDataQuery = libs.EquityMarketDataQuery(
        tag_string="symbol",
        tags=tickers,
        # n=moving_average_long_window+100,
        # start_time="-50m",
        start_time=start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_time=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        aggregate_timeframe="1m",
        aggregation_fn=InfluxAggregationType.LAST,
        add_pivot=True)
    technicalIndicatorStrategyManager = libs.TechnicalIndicatorStrategyManager(service_name=service_name,
                                                                               kafka_bootstrap_servers=bootstrap_servers,
                                                                               strategy=moving_average_stategy,
                                                                               live_portfolio=live_portfolio,
                                                                               query=equityMarketDataQuery,
                                                                               influx_client_manager=influx_client_manager,
                                                                               redis_client_manager=redis_client_manager,
                                                                               capital=capital,
                                                                               tenor=tenor_preceding,
                                                                               calendar=calendar,
                                                                               query_executor_data=libs.QueryExecutorData("-3hr"),
                                                                               sampling_frequency=60,
                                                                               start_datetime=start_dt,
                                                                               end_datetime=end_dt)

    asyncio.run(technicalIndicatorStrategyManager.run())
