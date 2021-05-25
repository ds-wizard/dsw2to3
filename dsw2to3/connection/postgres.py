import psycopg2
import psycopg2.extensions
import tenacity

from dsw2to3.config import PostgresConfig
from dsw2to3.errors import ERROR_HANDLER
from dsw2to3.logger import LOGGER


class PostgresDB:

    def __init__(self, config: PostgresConfig):
        self.config = config
        self.dsn = psycopg2.extensions.make_dsn(
            dsn=config.connection_string
        )
        self._connection = None

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.3),
        stop=tenacity.stop_after_attempt(3),
    )
    def _connect_db(self):
        LOGGER.debug(f'Creating connection to PostgreSQL database')
        connection = psycopg2.connect(dsn=self.dsn)
        # test connection
        with connection.cursor() as cursor:
            cursor.execute(query='SELECT 1;')
        connection.commit()
        self._connection = connection

    def connect(self):
        if not self._connection or self._connection.closed != 0:
            self._connect_db()

    @property
    def connection(self):
        self.connect()
        return self._connection

    def new_cursor(self):
        return self.connection.cursor()

    def reset(self):
        self.close()
        self.connect()

    def commit(self):
        self.connection.commit()

    def close(self):
        if self._connection:
            LOGGER.info(f'Closing connection to PostgreSQL database')
            self._connection.close()
        self._connection = None

    def execute(self, entity, instance):
        LOGGER.info(f'Executing INSERT INTO {entity.TABLE_NAME}'
                    f' (single)')
        try:
            with self.new_cursor() as cursor:
                cursor.execute(
                    query=entity.INSERT_QUERY,
                    vars=instance.query_vars(),
                )
        except Exception as e:
            ERROR_HANDLER.error(
                cause='PostgreSQL',
                message=f'Failed to insert {entity.__name__} ({e})'
                        f' with vars {instance.query_vars()}',
            )

    def execute_loop(self, entity, instances):
        with self.new_cursor() as cursor:
            for instance in instances:
                try:
                    cursor.execute(
                        query=entity.INSERT_QUERY,
                        vars=instance.query_vars(),
                    )
                except Exception as e:
                    ERROR_HANDLER.error(
                        cause='PostgreSQL',
                        message=f'Failed to insert {entity.__name__} ({e})'
                                f' with vars {instance.query_vars()}',
                    )

    def disable_triggers(self, table: str):
        with self.new_cursor() as cursor:
            cursor.execute(f'ALTER TABLE {table} DISABLE TRIGGER ALL;')

    def enable_triggers(self, table: str):
        with self.new_cursor() as cursor:
            cursor.execute(f'ALTER TABLE {table} ENABLE TRIGGER ALL;')
