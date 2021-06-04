import dataclasses
import datetime
import json
import uuid

from typing import List

from dsw2to3.config import Config
from dsw2to3.connection import PostgresDB, MongoDB, S3Storage
from dsw2to3.errors import ERROR_HANDLER
from dsw2to3.logger import LOGGER
from dsw2to3.migration.common import Migrator, MigrationOptions, insert_query
from dsw2to3.migration.entitites import Package, Template, TemplateFile, TemplateAsset


class DSWJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            ts = obj.isoformat(timespec='microseconds')
            return f'{ts}Z'
        return super().default(obj)


def _wrap_json(data):
    return json.dumps(data, cls=DSWJsonEncoder)


@dataclasses.dataclass
class ActionKey:
    COLLECTION = 'actionKeys'
    TABLE_NAME = 'action_key'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'organization_id',
            'type',
            'hash',
            'created_at',
        ]
    )

    uuid: str  # uuid PK
    organization_id: str  # uuid FK
    type: str  # varchar
    hash: str  # varchar
    created_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.organization_id,
            self.type,
            self.hash,
            self.created_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return ActionKey(
            uuid=doc.get('uuid'),
            type=doc.get('type'),
            hash=doc.get('hash'),
            organization_id=doc.get('organizationId'),
            created_at=doc.get('createdAt', now),
        )


@dataclasses.dataclass
class AuditEntry:
    COLLECTION = 'auditEntries'
    TABLE_NAME = 'audit'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'type',
            'organization_id',
            'instance_statistics',
            'package_id',
            'created_at',
        ]
    )

    type: str  # uuid PK
    organization_id: str  # uuid FK
    instance_statistics: str  # json
    package_id: str  # varchar
    created_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return f'{self.organization_id}_{uuid.uuid4()}'

    def query_vars(self):
        return (
            self.type,
            self.organization_id,
            _wrap_json(self.instance_statistics),
            self.package_id,
            self.created_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return AuditEntry(
            type=doc.get('type', 'ListPackagesAuditEntry'),
            organization_id=doc['organizationId'],
            instance_statistics=doc.get('instanceStatistics', None),
            package_id=doc.get('packageId', ''),
            created_at=doc.get('createdAt', now),
        )


@dataclasses.dataclass
class Organization:
    COLLECTION = 'organizations'
    TABLE_NAME = 'organization'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'organization_id',
            'name',
            'description',
            'email',
            'role',
            'token',
            'active',
            'logo',
            'created_at',
            'updated_at',
        ]
    )

    organization_id: str
    name: str
    description: str
    email: str
    role: str
    token: str
    active: bool
    logo: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    integrity_ok: bool = True

    def get_id(self):
        return self.organization_id

    def query_vars(self):
        return (
            self.organization_id,
            self.name,
            self.description,
            self.email,
            self.role,
            self.token,
            self.active,
            self.logo,
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Organization(
            organization_id=doc['organizationId'],
            name=doc['name'],
            description=doc['description'],
            email=doc['email'],
            role=doc['role'],
            token=doc['token'],
            active=doc['active'],
            logo=doc['logo'],
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class RegistryEntities:
    action_keys: list[ActionKey] = dataclasses.field(default_factory=list)
    audit_entries: list[AuditEntry] = dataclasses.field(default_factory=list)
    organizations: list[Organization] = dataclasses.field(default_factory=list)
    packages: list[Package] = dataclasses.field(default_factory=list)
    templates: list[Template] = dataclasses.field(default_factory=list)
    template_assets: list[TemplateAsset] = dataclasses.field(default_factory=list)
    template_files: list[TemplateFile] = dataclasses.field(default_factory=list)

    _result: list[str] = dataclasses.field(default_factory=list)

    LIST_ENTITY = {
        'action_keys': ActionKey,
        'audit_entries': AuditEntry,
        'organizations': Organization,
        'packages': Package,
        'templates': Template,
        'template_assets': TemplateAsset,
        'template_files': TemplateFile,
    }
    ENTITIES = [e for e in LIST_ENTITY.values()]
    ENTITY_LIST = {e.__name__: lst for lst, e in LIST_ENTITY.items()}

    def list_by_entity(self, entity) -> list:
        return getattr(self, self.ENTITY_LIST.get(entity.__name__))

    def set_list(self, entity, lst: list):
        return setattr(self, self.ENTITY_LIST.get(entity.__name__), lst)

    def valid_entries(self, entity):
        return (x for x in self.list_by_entity(entity) if x.integrity_ok is not False)

    def clear(self):
        for entity in self.ENTITIES:
            self.list_by_entity(entity).clear()

    def _check_uniqueness(self, list_name: str, entity):
        unique_set = set()
        for item in getattr(self, list_name):
            item_id = item.get_id()
            if item_id in unique_set:
                item.integrity_ok = False
                self._result.append(
                    f'Duplicate ID for {entity.__name__}: {item_id}'
                )
            unique_set.add(item_id)
        unique_set.clear()

    def _make_ids_set(self, entity) -> frozenset:
        return frozenset((
            item.get_id() for item in self.valid_entries(entity)
        ))

    def _check_reference(self, entity, target_entity, field_name: str, ids_set: frozenset):
        for item in self.valid_entries(entity):
            ref_id = getattr(item, field_name)
            if ref_id not in ids_set:
                item.integrity_ok = False
                self._result.append(
                    f'Missing {target_entity.__name__} ({field_name}={ref_id}) '
                    f'for {entity.__name__}: {item.get_id()}'
                )

    def _check_optional_reference(self, entity, target_entity, field_name: str, ids_set: frozenset):
        for item in self.valid_entries(entity):
            ref_id = getattr(item, field_name)
            if ref_id is not None and ref_id not in ids_set:
                item.integrity_ok = False
                self._result.append(
                    f'Missing {target_entity.__name__} ({field_name}={ref_id}) '
                    f'for {entity.__name__}: {item.get_id()}'
                )

    def _check_references(self) -> bool:
        prev_len = len(self._result)
        LOGGER.debug('  - collecting IDs of consistent entries')
        user_ids = self._make_ids_set(Organization)
        package_ids = self._make_ids_set(Package)
        template_ids = self._make_ids_set(Template)

        LOGGER.debug('  - checking references')
        self._check_reference(ActionKey, Organization, 'organization_id', user_ids)
        self._check_optional_reference(Package, Package, 'previous_package_id', package_ids)
        self._check_optional_reference(Package, Package, 'fork_of_package_id', package_ids)
        self._check_optional_reference(Package, Package, 'merge_checkpoint_package_id', package_ids)
        self._check_reference(TemplateAsset, Template, 'template_id', template_ids)
        self._check_reference(TemplateFile, Template, 'template_id', template_ids)

        LOGGER.debug(f'  - {len(self._result) - prev_len} new inconsistencies found')
        return prev_len != len(self._result)

    def check_integrity(self) -> list[str]:
        self._result.clear()
        # Uniqueness (PKs, UNIQUE)
        LOGGER.info('- checking ID uniqueness')
        for lst, e in self.LIST_ENTITY.items():
            self._check_uniqueness(list_name=lst, entity=e)
        # References (FKs)
        LOGGER.info('- checking references for the first time')
        check_again = self._check_references()
        i = 1
        while check_again:
            LOGGER.info(f'- checking references again (#{i})')
            i += 1
            check_again = self._check_references()
        return self._result


class RegistryMigrator(Migrator):

    # All tables but in specific order (bcs FK)
    _TABLES_CLEANUP = [
        ActionKey.TABLE_NAME,
        AuditEntry.TABLE_NAME,
        Organization.TABLE_NAME,
        TemplateAsset.TABLE_NAME,
        TemplateFile.TABLE_NAME,
        Template.TABLE_NAME,
        Package.TABLE_NAME,
    ]

    _LOAD_ENTITIES_SIMPLE = [
        ActionKey,
        AuditEntry,
        Organization,
        Package,
        Template,
    ]

    _LOAD_ENTITIES_NESTED = [
        (TemplateAsset, Template, 'assets'),
        (TemplateFile, Template, 'files'),
    ]

    _DEFAULT_TABLE_COUNTS = {
        ActionKey.TABLE_NAME: 0,
        AuditEntry.TABLE_NAME: 0,
        Organization.TABLE_NAME: 0,
        Package.TABLE_NAME: 0,
        Template.TABLE_NAME: 0,
        TemplateAsset.TABLE_NAME: 0,
        TemplateFile.TABLE_NAME: 0,
    }

    _INSERT_ARGS = [
        (Package, True),  # Package (FKs: Package)
        (Template, False),  # Template (FKs: -)
        (AuditEntry, False),  # Audit (FKs: -)
        (Organization, False),  # Questionnaire (FKs: -)
        (ActionKey, False),  # ActionKey (FKs: Questionnaire)
        (TemplateAsset, False),  # TemplateAsset (FKs: Template)
        (TemplateFile, False),  # TemplateFile (FKs: Template)
    ]

    def __init__(self, config: Config, options: MigrationOptions):
        super().__init__(config, options)
        self.entities = RegistryEntities()
        self.postgres = PostgresDB(config=config.postgres)
        self.mongo = MongoDB(config=config.mongo)
        self.s3 = S3Storage(config=config.s3)

    def cleanup_postgres(self):
        LOGGER.info(f'Cleaning up target database{self._dry_run_tag()}')
        try:
            with self.postgres.new_cursor() as cursor:
                for table in self._TABLES_CLEANUP:
                    cursor.execute(
                        query=f'DELETE FROM {table};',
                    )
                    rows_deleted = cursor.rowcount
                    LOGGER.debug(f'- deleted {rows_deleted} from {table}')
                if not self.options.dry_run:
                    self.postgres.commit()
        except Exception as e:
            ERROR_HANDLER.critical(
                cause='PostgreSQL',
                message=f'- failed to clean up PostgreSQL ({e})',
            )

    def cleanup_s3(self):
        LOGGER.info(f'Checking existing data in S3 bucket'
                    f'{self._dry_run_tag()}')
        if self.s3.bucket_exists():
            templates = self.s3.count_templates()
            LOGGER.debug(f'- there are {templates} templates in the S3 bucket')
            if templates > 0:
                LOGGER.info(f'- cleaning S3 bucket (deleting objects){self._dry_run_tag()}')
                if not self.options.dry_run:
                    self.s3.delete_templates()
        else:
            LOGGER.info(f'- creating S3 bucket{self._dry_run_tag()}')
            if not self.options.dry_run:
                self.s3.ensure_bucket()

    def load(self):
        LOGGER.info(f'Loading data from MongoDB')
        self.mongo.update_now()

        for entity in self._LOAD_ENTITIES_SIMPLE:
            self.entities.set_list(entity, self.mongo.load_list(entity=entity))

        for entity, parent_entity, field in self._LOAD_ENTITIES_NESTED:
            self.entities.set_list(entity, self.mongo.load_nested(
                source_entity=parent_entity,
                target_entity=entity,
                field=field,
            ))

    def check_integrity(self):
        LOGGER.info('Checking data integrity')
        issues = self.entities.check_integrity()
        for issue in issues:
            LOGGER.warning(f'- violation: {issue}')
        if not self.options.fix_integrity and len(issues) > 0:
            ERROR_HANDLER.error(
                cause='Integrity',
                message='- data integrity violated (see logs above). '
                        'You can skip invalid entries by re-run with '
                        'flag --fix-integrity.'
            )

    def _dry_run_tag(self):
        return ' [--dry-run]' if self.options.dry_run else ''

    def _run_insert(self, entity, disable_triggers: bool = False):
        instances = self.entities.list_by_entity(entity)
        ok_instances = [e for e in instances if e.integrity_ok is True]
        LOGGER.info(f'- executing INSERT INTO {entity.TABLE_NAME} '
                    f'({len(ok_instances)} of {len(instances)})')
        if disable_triggers:
            self.postgres.disable_triggers(entity.TABLE_NAME)
        self.postgres.execute_loop(entity=entity, instances=ok_instances)
        if disable_triggers:
            self.postgres.enable_triggers(entity.TABLE_NAME)

    def insert(self):
        LOGGER.info(f'Inserting data to target PostgreSQL '
                    f'database{self._dry_run_tag()}')

        for entity, disable_triggers in self._INSERT_ARGS:
            self._run_insert(entity=entity, disable_triggers=disable_triggers)

        try:
            LOGGER.info(f'- committing transaction{self._dry_run_tag()}')
            if not self.options.dry_run:
                self.postgres.commit()
        except Exception as e:
            ERROR_HANDLER.critical(
                cause='PostgreSQL',
                message=f'- failed to commit transaction: {e}'
            )

    def migrate_template_assets(self):
        counter = 0
        skipped = 0
        assets = list(self.entities.valid_entries(TemplateAsset))  # type: List[TemplateAsset]
        LOGGER.info(f'Migrating assets ({len(assets)} '
                    f'assets){self._dry_run_tag()}')
        for asset in assets:
            data = self.mongo.fetch_asset(asset.original_uuid)
            if data is None:
                LOGGER.warning(
                    f' - no data found for asset {asset.uuid} '
                    f'(original UUID {asset.original_uuid}),'
                    f'cannot be transferred to S3 storage - skipping'
                )
                skipped += 1
                continue
            if not self.options.dry_run:
                self.s3.store_template_asset(
                    template_id=asset.template_id,
                    file_name=asset.uuid,
                    content_type=asset.content_type,
                    data=data
                )
            counter += 1
        LOGGER.info(f'- {counter} template assets stored in S3 '
                    f'({skipped} not found and skipped)')

    def finish(self):
        LOGGER.debug('Closing connections')
        self.postgres.close()

    def migrate(self):
        LOGGER.info('Starting to clean up migration targets')
        self.cleanup_postgres()
        self.cleanup_s3()
        LOGGER.info('Cleaning up migration targets finished')

        LOGGER.info('Starting database migration')
        self.load()
        self.check_integrity()
        self.insert()
        LOGGER.info('Database migration finished')

        LOGGER.info('Starting file storage migration')
        self.migrate_template_assets()
        LOGGER.info('File storage migration finished')
