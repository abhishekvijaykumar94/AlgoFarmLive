import asyncio

from algoLibs.file_processors import TokenMappingProcessor
from algoLibs.market_data_stream import LiveMarketDataGreekCalculatorConsumer
from algoLibs.market_operations import EquityMarketDataPickleOperations, EqOptionGreeksDataInfluxOperations
from algoLibs.utils import PropertyManager, CommonUtils

if __name__ == '__main__':
    api_key = PropertyManager.getValue('apikey')
    greekCalculatorKafkaTopic = PropertyManager.getValue('smartApi.greekCalculator')
    boostrapServers = PropertyManager.getValue('boostrap.servers')
    pickleOutputPath = CommonUtils.getFilePathOutputDirectory()
    marketDataOperations = EquityMarketDataPickleOperations()
    batch_size=PropertyManager.getValue('batch_size')

    tokenProcessor = TokenMappingProcessor('Token_mapping.csv',
                                           CommonUtils.getFilePathFromDataDirectory('Token_mapping.csv'))
    derivativeTokenList = tokenProcessor.get_option_tokens()
    underlyingTokenList = tokenProcessor.get_index_underlying_tokens()
    tokenSymbolMap = tokenProcessor.get_Token_to_Symbol_Dict()
    marketDataOperations = EqOptionGreeksDataInfluxOperations()
    smartApiPickleMarketDataConsumer = LiveMarketDataGreekCalculatorConsumer(greekCalculatorKafkaTopic,boostrapServers,
                                                                             marketDataOperations,
                                                                             "smartApiLiveMarketData",
                                                                             derivativeTokenList,underlyingTokenList,
                                                                             tokenSymbolMap)
    print("Initiating Pickle Market Data Consumer on Topic:",greekCalculatorKafkaTopic)
    asyncio.run(smartApiPickleMarketDataConsumer.consume())

