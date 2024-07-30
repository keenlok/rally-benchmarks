from dbrally.client.context import RequestContextHolder
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from motor.motor_asyncio import AsyncIOMotorClient as MotorClient
from dbrally.exceptions import ConfigError


def get_conninfo(hosts, kwargs):
    # TODO: enforce mongo
    # TODO: handle if the credentials from userspec works
    if hosts is None:
        hosts = [{"host": "localhost"}]

    print(hosts, kwargs)
    scheme = kwargs.get('scheme', 'mongodb+srv')
    host_str = ",".join([host.get('host') for host in hosts])
    user_spec = kwargs.get('userspec', '')
    url_prefix = hosts[0].get('url_prefix', '/')
    url_query = hosts[0].get('url_query')
    conninfo = f"{scheme}://{user_spec}@{host_str}{url_prefix}?{url_query}"
    print(conninfo)
    return conninfo


class RallySyncMongoClient(MongoClient, RequestContextHolder):
    def __init__(self, hosts=None, *args, **kwargs):
        conninfo = get_conninfo(hosts, kwargs)
        try:
            super().__init__(host=conninfo,
                             server_api=ServerApi('1'))
        except Exception as e:
            raise ConfigError(f"Fail to connect to Mongo {e}")


class RallyAsyncMongoClient(MotorClient, RequestContextHolder):
    def __init__(self, hosts=None, *args, **kwargs):
        conninfo = get_conninfo(hosts, kwargs)
        try:
            super().__init__(host=conninfo,
                             server_api=ServerApi('1'))
        except Exception as e:
            raise ConfigError(f"Fail to connect to Mongo {e}")

