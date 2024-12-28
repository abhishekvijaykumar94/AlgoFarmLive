from algoLibs import email_to_oneself, InfluxDBClientManager, AppConstants
from algoLibs.utils.property_manager import PropertyManager

from algoFarmAdapter import MarketDataFeeder, SmartApiConnectionManager
from algoFarmAdapter.market_data.smart_api_market_data_subscriber import SmartApiMarketDataSubscriber

if __name__ == '__main__':
    ### Check holiday for today date..If today is holiday the program will exit
    # if check_holiday():
    #     email_to_oneself("Today is holiday..Enjoy!")
    #     sys.exit()

    smart_api_market_data_subscriber = SmartApiMarketDataSubscriber()
    token_list = smart_api_market_data_subscriber.get_token_subscription_list()
    message = "Starting script for today for {} tickers /n".format(len(token_list))
    message += "Tokens are :"
    message += ",".join([item['tokens'][0] for item in token_list])
    email_to_oneself(message)
    api_key = PropertyManager.getValue(AppConstants.API_KEY)
    liveMarketDataKafkaTopic = PropertyManager.getValue(AppConstants.SMARTAPI_LIVE_MARKET_DATA)
    boostrapServers = PropertyManager.getValue(AppConstants.BOOTSTRAP_SERVERS)
    influxDBClientManager = InfluxDBClientManager()
    connectionManager = SmartApiConnectionManager(api_key)
    data,feedToken = connectionManager.generate_session()
    batch_size = PropertyManager.getValue(AppConstants.BATCH_SIZE)
    smartApiMarketDataFeeder = MarketDataFeeder(api_key,data,liveMarketDataKafkaTopic,boostrapServers,feedToken,token_list,int(batch_size))
    # smartApiMarketDataConsumer = MarketDataConsumer(liveMarketDataKafkaTopic,boostrapServers,"smartApiLiveMarketData",influxDBClientManager)
    smartApiMarketDataFeeder.start()

