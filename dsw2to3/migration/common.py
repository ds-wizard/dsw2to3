import dataclasses

from dsw2to3.config import Config


def insert_query(table_name: str, fields: list[str]):
    field_names = ', '.join(fields)
    field_placeholders = ', '.join('%s' for _ in fields)
    return f'INSERT INTO {table_name} ({field_names}) VALUES ({field_placeholders});'


def validate_mongo_doc(document: dict, required_fields: list[str]):
    missing = [field for field in required_fields
               if field not in document.keys()]
    if len(missing) > 0:
        raise RuntimeError(f'Missing fields: {missing}')


@dataclasses.dataclass
class MigrationOptions:
    dry_run: bool
    skip_pre_check: bool
    fix_integrity: bool


class Migrator:

    def __init__(self, config: Config, options: MigrationOptions):
        self.config = config
        self.options = options

    def pre_check(self) -> bool:
        pass

    def migrate(self):
        pass

    def finish(self):
        pass
