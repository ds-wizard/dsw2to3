import dataclasses
import datetime
import json
import psycopg2
import pymongo

from dsw2to3.config import Config
from dsw2to3.logger import LOGGER
from dsw2to3.migration.common import insert_query, Migrator, MigrationOptions


@dataclasses.dataclass
class ActionKey:
    INSERT_QUERY = insert_query(
        table_name='action_key',
        fields=[
            'uuid',
            'organization_id',
            'type',
            'hash',
            'created_at',
        ]
    )

    uuid: str
    organization_id: str
    type: str
    hash: str
    created_at: datetime.datetime

    def query_vars(self):
        return (
            self.uuid,
            self.organization_id,
            self.type,
            self.hash,
            self.created_at,
        )


@dataclasses.dataclass
class AuditEntry:
    INSERT_QUERY = insert_query(
        table_name='audit',
        fields=[
            'type',
            'organization_id',
            'instance_statistics',
            'package_id',
            'created_at',
        ]
    )

    type: str
    organization_id: str
    instance_statistics: dict
    package_id: str
    created_at: datetime.datetime

    def query_vars(self):
        return (
            self.type,
            self.organization_id,
            json.dumps(self.instance_statistics),
            self.package_id,
            self.created_at,
        )


@dataclasses.dataclass
class Organization:

    INSERT_QUERY = insert_query(
        table_name='organization',
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


@dataclasses.dataclass
class Package:

    INSERT_QUERY = insert_query(
        table_name='package',
        fields=[
            'id',
            'name',
            'organization_id',
            'km_id',
            'version',
            'metamodel_version',
            'description',
            'readme',
            'license',
            'previous_package_id',
            'fork_of_package_id',
            'merge_checkpoint_package_id',
            'events',
            'created_at',
        ]
    )

    id: str
    name: str
    organization_id: str
    km_id: str
    version: str
    metamodel_version: str
    description: str
    readme: str
    license: str
    previous_package_id: str
    fork_of_package_id: str
    merge_checkpoint_package_id: str
    events: list
    created_at: datetime.datetime

    def query_vars(self):
        return (
            self.id,
            self.name,
            self.organization_id,
            self.km_id,
            self.version,
            self.metamodel_version,
            self.description,
            self.readme,
            self.license,
            self.previous_package_id,
            self.fork_of_package_id,
            self.merge_checkpoint_package_id,
            json.dumps(self.events),
            self.created_at,
        )


@dataclasses.dataclass
class Template:

    INSERT_QUERY = insert_query(
        table_name='template',
        fields=[
            'id',
            'name',
            'organization_id',
            'template_id',
            'version',
            'metamodel_version',
            'description',
            'readme',
            'license',
            'allowed_packages',
            'recommended_package_id',
            'formats',
            'files',
            'assets',
            'created_at',
        ]
    )

    id: str
    name: str
    organization_id: str
    template_id: str
    version: str
    metamodel_version: str
    description: str
    readme: str
    license: str
    allowed_packages: list
    recommended_package_id: str
    formats: list
    files: list
    assets: list
    created_at: datetime.datetime

    def query_vars(self):
        return (
            self.id,
            self.name,
            self.organization_id,
            self.template_id,
            self.version,
            self.metamodel_version,
            self.description,
            self.readme,
            self.license,
            json.dumps(self.allowed_packages),
            self.recommended_package_id,
            json.dumps(self.formats),
            json.dumps(self.files),
            json.dumps(self.assets),
            self.created_at,
        )


@dataclasses.dataclass
class RegistryEntities:
    action_keys: list[ActionKey] = dataclasses.field(default_factory=list)
    audit_entries: list[AuditEntry] = dataclasses.field(default_factory=list)
    organizations: list[Organization] = dataclasses.field(default_factory=list)
    packages: list[Package] = dataclasses.field(default_factory=list)
    templates: list[Template] = dataclasses.field(default_factory=list)

    def clear(self):
        self.action_keys.clear()
        self.audit_entries.clear()
        self.organizations.clear()
        self.packages.clear()
        self.templates.clear()


class RegistryMigrator(Migrator):

    def __init__(self, config: Config, options: MigrationOptions):
        super().__init__(config, options)
        self.entities = RegistryEntities()

    def load(self):
        LOGGER.info(f'Loading data from MongoDB: {self.config.mongo.host}:{self.config.mongo.port}/{self.config.mongo.database}')
        mongo_client = pymongo.MongoClient(**self.config.mongo.mongo_client_kwargs)
        db = mongo_client[self.config.mongo.database]

        for doc in db['actionKeys'].find():
            self.entities.action_keys.append(ActionKey(
                uuid=doc['uuid'],
                organization_id=doc['organizationId'],
                type=doc['type'],
                hash=doc['hash'],
                created_at=doc['createdAt'],
            ))
        LOGGER.info(f'- Loaded {len(self.entities.action_keys)} actions keys')

        for doc in db['auditEntries'].find():
            self.entities.audit_entries.append(AuditEntry(
                type=doc.get('type', 'ListPackagesAuditEntry'),
                organization_id=doc['organizationId'],
                instance_statistics=doc.get('instanceStatistics', None),
                package_id=doc.get('packageId', ''),
                created_at=doc['createdAt'],
            ))
        LOGGER.info(f'- Loaded {len(self.entities.audit_entries)} audit entries')

        for doc in db['organizations'].find():
            self.entities.organizations.append(Organization(
                organization_id=doc['organizationId'],
                name=doc['name'],
                description=doc['description'],
                email=doc['email'],
                role=doc['role'],
                token=doc['token'],
                active=doc['active'],
                logo=doc['logo'],
                created_at=doc['createdAt'],
                updated_at=doc['updatedAt'],
            ))
        LOGGER.info(f'- Loaded {len(self.entities.organizations)} organizations')

        for doc in db['packages'].find():
            self.entities.packages.append(Package(
                id=doc['id'],
                name=doc['name'],
                organization_id=doc['organizationId'],
                km_id=doc['kmId'],
                version=doc['version'],
                metamodel_version=doc['metamodelVersion'],
                description=doc['description'],
                readme=doc['readme'],
                license=doc['license'],
                previous_package_id=doc['previousPackageId'],
                fork_of_package_id=doc['forkOfPackageId'],
                merge_checkpoint_package_id=doc['mergeCheckpointPackageId'],
                events=doc['events'],
                created_at=doc['createdAt'],
            ))
        LOGGER.info(f'- Loaded {len(self.entities.packages)} packages')

        for doc in db['templates'].find():
            self.entities.templates.append(Template(
                id=doc['id'],
                name=doc['name'],
                organization_id=doc['organizationId'],
                template_id=doc['templateId'],
                version=doc['version'],
                metamodel_version=doc['metamodelVersion'],
                description=doc['description'],
                readme=doc['readme'],
                license=doc['license'],
                allowed_packages=doc['allowedPackages'],
                recommended_package_id=doc['recommendedPackageId'],
                formats=doc['formats'],
                files=doc['files'],
                assets=doc['assets'],
                created_at=doc['createdAt'],
            ))
        LOGGER.info(f'- Loaded {len(self.entities.packages)} templates')
        LOGGER.info('Loading data from MongoDB finished')

    def insert(self):
        LOGGER.info(f'Connecting to PostgreSQL: {self.config.postgres.host}:{self.config.postgres.port}/{self.config.postgres.database}')
        conn = psycopg2.connect(self.config.postgres.connection_string)
        cursor = conn.cursor()

        LOGGER.info('Executing INSERT INTO action_key')
        for action_key in self.entities.action_keys:
            cursor.execute(
                query=ActionKey.INSERT_QUERY,
                vars=action_key.query_vars(),
            )

        LOGGER.info('Executing INSERT INTO audit')
        for audit_entry in self.entities.audit_entries:
            cursor.execute(
                query=AuditEntry.INSERT_QUERY,
                vars=audit_entry.query_vars(),
            )

        LOGGER.info('Executing INSERT INTO organization')
        for organization in self.entities.organizations:
            cursor.execute(
                query=Organization.INSERT_QUERY,
                vars=organization.query_vars(),
            )

        LOGGER.info('Executing INSERT INTO package')
        for package in self.entities.packages:
            cursor.execute(
                query=Package.INSERT_QUERY,
                vars=package.query_vars(),
            )

        LOGGER.info('Executing INSERT INTO template')
        for template in self.entities.templates:
            cursor.execute(
                query=Template.INSERT_QUERY,
                vars=template.query_vars(),
            )

        LOGGER.info('Committing transaction')
        conn.commit()
        LOGGER.info('Closing PostgreSQL connection')
        cursor.close()
        conn.close()

    def migrate(self):
        self.load()
        self.insert()
        LOGGER.info('Registry database migration finished')
