import asyncio

from algoFarmAdapter.market_data.daily_market_data_downloader import DailyMarketDataDownloader
from algoLibs import PropertyManager, InfluxDBClientManager

if __name__ == '__main__':

    start_dateTime = '2025-01-24 00:00:00'
    end_dateTime = '2025-01-25 00:00:00'
    bucket_name = PropertyManager.getValue('s3bucket')
    dailyMarketDownloader = DailyMarketDataDownloader(start_dateTime, end_dateTime,
                                                                                  InfluxDBClientManager(),
                                                                                  bucket_name,10)
    # asyncio.run(dailyMarketDownloader.run())
    dailyMarketDownloader.run()