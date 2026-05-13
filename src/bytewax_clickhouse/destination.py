from bytewax.outputs import DynamicSink, StatelessSinkPartition
from clickhouse_connect import get_client


class ClickhouseSinkPartition(StatelessSinkPartition):
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        table: str,
    ):
        self._client = get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        self._table = table

    def write_batch(self, chunks: list):
        if not chunks:
            return
        try:
            # Берём имена колонок из первого словаря (порядок сохраняется)
            column_names = list(chunks[0].keys())
            # Преобразуем каждый словарь в список значений в том же порядке
            rows = []
            for item in chunks:
                row = [item.get(col) for col in column_names]
                rows.append(row)
            # Явно указываем column_names
            self._client.insert(self._table, rows, column_names=column_names)
        except Exception as e:
            print(f'ClickHouse insert error: {type(e).__name__}: {e}')
            import traceback

            traceback.print_exc()

    def close(self):
        if hasattr(self, '_client'):
            self._client.close()


class ClickhouseSync(DynamicSink):
    def __init__(
        self,
        host: str = 'localhost',
        port: int = '8123',
        username: str = '',
        password: str = '',
        database: str = 'default',
        table: str = '',
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        assert table
        self.table = table

    def build(self, *args) -> ClickhouseSinkPartition:
        return ClickhouseSinkPartition(
            self.host,
            self.port,
            self.username,
            self.password,
            self.database,
            self.table,
        )
