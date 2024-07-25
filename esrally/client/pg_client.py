from psycopg import AsyncConnection as AsyncPostgres
import psycopg
from psycopg import errors

from esrally.client.context import RequestContextHolder


class RallyAsyncPostgres(AsyncPostgres, RequestContextHolder):
    def __init__(self, hosts=None, *args, **kwargs):
        print(hosts)
        # print(args)
        # print(kwargs)
        if hosts is None:
            hosts = [{"host": "localhost"}]

        # TODO: enforce postgres
        # TODO: handle if the credentials from userspec works
        scheme = kwargs.get('scheme', 'postgres')
        host_str = ",".join([host.get('host') for host in hosts])
        user_spec = kwargs.get('userspec', '')
        url_prefix = hosts[0].get('url_prefix', '/postgres')
        conninfo = f"{scheme}://{user_spec}@{host_str}{url_prefix}"
        print(conninfo)

        # create the connection synchronously,
        # but we still want the Asynchronous PG client
        try:
            print("try creating postgres connection")
            conn = psycopg.connect(conninfo)
            print("Postgres client created successfully")
            super().__init__(pgconn=conn.pgconn)
            print("Async client created successfully")
        except psycopg.Error as e:
            print(e)
            raise e
