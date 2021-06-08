import dataclasses
import yaml

from psycopg2.extensions import parse_dsn  # type: ignore
from typing import Optional, List


class MissingConfigurationError(Exception):

    def __init__(self, missing: List[str]):
        self.missing = missing


@dataclasses.dataclass
class MongoConfig:
    host: str
    port: int
    database: str
    auth_enabled: bool
    username: Optional[str]
    password: Optional[str]

    @property
    def mongo_client_kwargs(self):
        kwargs = {
            'host': self.host, 'port': self.port
        }
        if self.auth_enabled:
            kwargs['username'] = self.username
            kwargs['password'] = self.password
            kwargs['authSource'] = self.database
        return kwargs

    def __str__(self):
        return f'{self.host}:{self.port}/{self.database}'


@dataclasses.dataclass
class PostgresConfig:
    connection_string: str


@dataclasses.dataclass
class S3Config:
    url: str
    username: str
    password: str
    bucket: str


@dataclasses.dataclass
class Config:
    mongo: MongoConfig
    postgres: PostgresConfig
    s3: S3Config


class ConfigParser:
    DEFAULTS = {
        'database': {
            'host': 'mongo',
            'port': 27017,
            'authEnabled': False,
            'username': None,
            'password': None,
        },
        's3': {
            'url': 'http://minio:9000',
            'bucket': 'engine-wizard',
            'username': 'minio',
            'password': 'minioPassword'
        }
    }
    REQUIRED = [
        ['database', 'connectionString'],
    ]

    def __init__(self):
        self.cfg = dict()

    @staticmethod
    def can_read(content):
        try:
            yaml.load(content, Loader=yaml.FullLoader)
            return True
        except Exception:
            return False

    def read_file(self, fp):
        self.cfg = yaml.load(fp, Loader=yaml.FullLoader)

    def read_string(self, content):
        self.cfg = yaml.load(content, Loader=yaml.FullLoader)

    def has(self, *path):
        x = self.cfg
        for p in path:
            if not hasattr(x, 'keys') or p not in x.keys():
                return False
            x = x[p]
        return True

    def _get_default(self, *path):
        x = self.DEFAULTS
        for p in path:
            x = x[p]
        return x

    def get_or_default(self, *path):
        x = self.cfg
        for p in path:
            if not hasattr(x, 'keys') or p not in x.keys():
                return self._get_default(*path)
            x = x[p]
        return x

    def validate(self):
        missing = []
        for path in self.REQUIRED:
            if not self.has(*path):
                missing.append('.'.join(path))
        if len(missing) > 0:
            raise MissingConfigurationError(missing)

    @property
    def mongo(self):
        return MongoConfig(
            host=self.get_or_default('database', 'host'),
            port=self.get_or_default('database', 'port'),
            database=self.get_or_default('database', 'databaseName'),
            auth_enabled=self.get_or_default('database', 'authEnabled'),
            username=self.get_or_default('database', 'username'),
            password=self.get_or_default('database', 'password'),
        )

    @property
    def postgres(self):
        connection_string = self.get_or_default('database', 'connectionString')
        parsed = parse_dsn(connection_string)
        return PostgresConfig(
            connection_string=connection_string,
        )

    @property
    def s3(self):
        return S3Config(
            url=self.get_or_default('s3', 'url'),
            bucket=self.get_or_default('s3', 'bucket'),
            username=self.get_or_default('s3', 'username'),
            password=self.get_or_default('s3', 'password'),
        )

    @property
    def config(self):
        return Config(
            mongo=self.mongo,
            postgres=self.postgres,
            s3=self.s3,
        )
