from bytewax.outputs import DynamicSink, StatelessSinkPartition
from clickhouse_connect import get_client


class ClickhouseSinkPartition(StatelessSinkPartition):
    def __init__(self, host: str, port: int, username: str, password: str, database: str, table: str):
        self._client = get_client(host, port, username, password, database)
        self._table = table

    def write_batch(self, chunks: list[any]):
        self._client.insert(self.table, chunks)


class ClickhouseSync(DynamicSink):
    def __init__(self, 
            host: str = 'localhost', 
            port: int = '8123', 
            username: str = '',
            password: str = '',
            database: str = 'default',
            table: str = '',
            ):
        self.host = host
        self.port = port
        self.username = username,
        self.password = password,
        self.database = database,
        assert(table)
        self.table = table

    def build(self, *args) -> ClickhouseSinkPartition:
        return ClickhouseSinkPartition(self.host, self.port, self.username, self.password, self.database, self.table)
    
