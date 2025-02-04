from algoFarmAdapter import SmartApiConnectionManager, SmartAPIOrderListener
from algoLibs import AppConstants
from algoLibs.utils.property_manager import PropertyManager

if __name__ == '__main__':

    api_key = PropertyManager.getValue(AppConstants.API_KEY)
    kafka_topic = PropertyManager.getValue(AppConstants.SMARTAPI_LIVE_MARKET_DATA)
    boostrapServers = PropertyManager.getValue(AppConstants.BOOTSTRAP_SERVERS)
    connectionManager = SmartApiConnectionManager(api_key)
    data,feedToken = connectionManager.generate_session()
    batch_size = PropertyManager.getValue(AppConstants.BATCH_SIZE)
    smart_api_order_listener = SmartAPIOrderListener(api_key=api_key,
                                                     session_data=data,
                                                     feed_token=feedToken,
                                                     kafka_topic=kafka_topic,
                                                     bootstrap_servers=boostrapServers
                                                     )
    smart_api_order_listener.start()

