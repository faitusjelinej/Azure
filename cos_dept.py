from azure.cosmos.aio import CosmosClient as cosmos_client 
from azure.cosmos import PartitionKey, exceptions
import asyncio #asyncronos IO
import datafile


endpoint = "https://cosmosdbdemofaitus.documents.azure.com:443/"
key = "Bx4umybrTXcuPNvb96AFSaFXSoE08RWqtV51vDYL3L8LNzROl4AJ3KMPvEdJ3E228qAyMXtNs6w3ACDbEI9lHg=="



database_name = 'department'
container_name = 't_department'


async def get_or_create_db(client, database_name):
    print("Creating database")
    return await client.create_database(database_name)



async def get_or_create_container(database_obj, container_name):
    print("Creating container with name as partition key")
    return await database_obj.create_container(
        id=container_name,
        partition_key=PartitionKey(path="/name"),
        offer_throughput=400)



async def populate_container_items(container_obj, items_to_create):
    for dept_item in items_to_create:
        inserted_item = await container_obj.create_item(body=dept_item)
        print("Inserted item for %s dept. Item Id: %s" %(inserted_item['name'], inserted_item['id']))



async def read_items(container_obj, items_to_read):
    for dept in items_to_read:
        item_response = await container_obj.read_item(item=dept['id'], partition_key=dept['name'])
        print('Id {0} Name {1} location {2} Count {3}'.format(item_response['id'], item_response['name'],item_response['location'],item_response['count']))



async def run_sample(): 
    async with cosmos_client(endpoint, credential = key) as client:
        database_obj = await get_or_create_db(client, database_name)
        container_obj = await get_or_create_container(database_obj, container_name)
        dept_items_to_create = [datafile.get_IT(), datafile.get_HR(), datafile.get_Finance()]
        await populate_container_items(container_obj, dept_items_to_create)  
        await read_items(container_obj, dept_items_to_create)
          

if __name__=="__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_sample())







