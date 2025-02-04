import asyncio

import algoLibs as libs
import algoFarmAdapter as adapter

if __name__ == '__main__':
    service_name="execution_handler"
    bootstrap_servers = libs.PropertyManager.getValue(libs.AppConstants.BOOTSTRAP_SERVERS)
    api_key = libs.PropertyManager.getValue(libs.AppConstants.API_KEY)
    executionHandler = adapter.ExecutionHandler(service_name,bootstrap_servers,api_key)

    asyncio.run(executionHandler.run())