import dataclasses
import datetime
import json
import uuid

from typing import Optional, List

from dsw2to3.migration.common import insert_query
from dsw2to3.logger import LOGGER


class DSWJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            ts = obj.isoformat(timespec='microseconds')
            return f'{ts}Z'
        return super().default(obj)


def _wrap_json(data):
    return json.dumps(data, cls=DSWJsonEncoder)


@dataclasses.dataclass
class ACLGroup:
    TABLE_NAME = 'acl_group'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'id',
            'name',
            'description',
        ]
    )

    id: str  # varchar PK
    name: str  # varchar
    description: str  # varchar
    integrity_ok: bool = True

    def get_id(self):
        return self.id

    def query_vars(self):
        return (
            self.id,
            self.name,
            self.description,
        )


@dataclasses.dataclass
class ActionKey:
    COLLECTION = 'actionKeys'
    TABLE_NAME = 'action_key'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'user_id',
            'type',
            'hash',
            'created_at',
        ]
    )

    uuid: str  # uuid PK
    user_id: str  # uuid FK??
    type: str  # varchar
    hash: str  # varchar
    created_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.user_id,
            self.type,
            self.hash,
            self.created_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return ActionKey(
            uuid=doc['uuid'],
            type=doc['type'],
            hash=doc['hash'],
            user_id=doc['userId'],
            created_at=doc.get('createdAt', now),
        )


@dataclasses.dataclass
class AppConfig:
    COLLECTION = 'appConfigs'
    TABLE_NAME = 'app_config'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'id',
            'organization',
            'authentication',
            'privacy_and_support',
            'dashboard',
            'look_and_feel',
            'registry',
            'knowledge_model',
            'questionnaire',
            'template',
            'submission',
            'created_at',
            'updated_at',
        ]
    )

    _DEFAULT_KM = {
        'public': {
            'enabled': False,
            'packages': [],
        },
    }

    # id: int  # bigint PK
    organization: dict  # json
    authentication: dict  # json
    privacy_and_support: dict  # json
    dashboard: dict  # json
    look_and_feel: dict  # json
    registry: dict  # json
    knowledge_model: dict  # json
    questionnaire: dict  # json
    template: dict  # json
    submission: dict  # json
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return 1

    def query_vars(self):
        return (
            self.get_id(),
            _wrap_json(self.organization),
            _wrap_json(self.authentication),
            _wrap_json(self.privacy_and_support),
            _wrap_json(self.dashboard),
            _wrap_json(self.look_and_feel),
            _wrap_json(self.registry),
            _wrap_json(self.knowledge_model),
            _wrap_json(self.questionnaire),
            _wrap_json(self.template),
            _wrap_json(self.submission),
            self.created_at,
            self.updated_at,
        )

    def _migrate(self):
        self.questionnaire['questionnaireSharing']['anonymousEnabled'] = False

    @classmethod
    def from_mongo(cls, doc: dict, now: datetime.datetime):
        app_config = AppConfig(
            organization=doc['organization'],
            authentication=doc['authentication'],
            privacy_and_support=doc['privacyAndSupport'],
            dashboard=doc['dashboard'],
            look_and_feel=doc['lookAndFeel'],
            registry=doc['registry'],
            knowledge_model=cls._DEFAULT_KM,
            questionnaire=doc['questionnaire'],
            template=doc['template'],
            submission=doc['submission'],
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )
        app_config._migrate()
        return app_config


@dataclasses.dataclass
class BookReference:
    COLLECTION = 'bookReferences'
    TABLE_NAME = 'book_reference'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'short_uuid',
            'book_chapter',
            'content',
            'created_at',
            'updated_at',
        ]
    )

    short_uuid: str  # varchar PK
    book_chapter: str  # varchar
    content: str  # varchar
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def query_vars(self):
        return (
            self.short_uuid,
            self.book_chapter,
            self.content,
            self.created_at,
            self.updated_at,
        )

    def get_id(self):
        return self.short_uuid

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return BookReference(
            short_uuid=doc['shortUuid'],
            book_chapter=doc['bookChapter'],
            content=doc['content'],
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class Branch:
    COLLECTION = 'branches'
    TABLE_NAME = 'branch'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'name',
            'km_id',
            'metamodel_version',
            'previous_package_id',
            'events',
            'owner_uuid',
            'created_at',
            'updated_at',
        ]
    )

    uuid: str  # uuid PK
    name: str  # varchar
    km_id: str  # varchar
    metamodel_version: int  # integer
    previous_package_id: str  # varchar
    events: list  # json
    owner_uuid: str  # uuid FK??
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.name,
            self.km_id,
            self.metamodel_version,
            self.previous_package_id,
            _wrap_json(self.events),
            self.owner_uuid,
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Branch(
            uuid=doc['uuid'],
            name=doc['name'],
            km_id=doc['kmId'],
            metamodel_version=doc['metamodelVersion'],
            previous_package_id=doc['previousPackageId'],
            events=doc['events'],
            owner_uuid=doc['ownerUuid'],
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class Document:
    COLLECTION = 'documents'
    TABLE_NAME = 'document'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'name',
            'state',
            'durability',
            'questionnaire_uuid',
            'questionnaire_event_uuid',
            'questionnaire_replies_hash',
            'template_id',
            'format_uuid',
            'metadata',
            'creator_uuid',
            'retrieved_at',
            'finished_at',
            'created_at',
        ]
    )

    uuid: str  # uuid PK
    name: str  # varchar
    state: str  # varchar
    durability: str  # varchar
    questionnaire_uuid: str  # uuid FK??
    questionnaire_event_uuid: str  # uuid
    questionnaire_replies_hash: int  # bigint
    template_id: str  # varchar FK??
    format_uuid: str  # uuid
    metadata: dict  # json
    creator_uuid: str  # uuid FK??
    retrieved_at: Optional[datetime.datetime]  # timestamp+tz
    finished_at: Optional[datetime.datetime]  # timestamp+tz
    created_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.name,
            self.state,
            self.durability,
            self.questionnaire_uuid,
            self.questionnaire_event_uuid,
            self.questionnaire_replies_hash,
            self.template_id,
            self.format_uuid,
            _wrap_json(self.metadata),
            self.creator_uuid,
            self.retrieved_at,
            self.finished_at,
            self.created_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Document(
            uuid=doc['uuid'],
            name=doc['name'],
            state=doc['state'],
            durability=doc['durability'],
            questionnaire_uuid=doc['questionnaireUuid'],
            questionnaire_event_uuid=doc['questionnaireEventUuid'],
            questionnaire_replies_hash=doc['questionnaireRepliesHash'],
            template_id=doc['templateId'],
            format_uuid=doc['formatUuid'],
            metadata=doc['metadata'],
            creator_uuid=doc['creatorUuid'],
            retrieved_at=doc.get('retrievedAt', None),
            finished_at=doc.get('finishedAt', None),
            created_at=doc.get('createdAt', now),
        )


@dataclasses.dataclass
class Feedback:
    COLLECTION = 'feedbacks'
    TABLE_NAME = 'feedback'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'issue_id',
            'question_uuid',
            'package_id',
            'title',
            'content',
            'created_at',
            'updated_at',
        ]
    )

    uuid: str  # uuid PK
    issue_id: int  # integer
    question_uuid: str  # uuid
    package_id: str  # varchar FK??
    title: str  # varchar
    content: str  # varchar
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.issue_id,
            self.question_uuid,
            self.package_id,
            self.title,
            self.content,
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Feedback(
            uuid=doc['uuid'],
            issue_id=doc['issueId'],
            question_uuid=doc['questionUuid'],
            package_id=doc['packageId'],
            title=doc['title'],
            content=doc['content'],
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class KMMigration:
    COLLECTION = 'kmMigrations'
    TABLE_NAME = 'knowledge_model_migration'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'branch_uuid',
            'metamodel_version',
            'migration_state',
            'branch_previous_package_id',
            'target_package_id',
            'branch_events',
            'target_package_events',
            'result_events',
            'current_knowledge_model',
        ]
    )

    branch_uuid: str  # uuid PK
    metamodel_version: int  # integer
    migration_state: dict  # json
    branch_previous_package_id: str  # varchar FK??
    target_package_id: str  # varchar FK??
    branch_events: list  # json
    target_package_events: list  # json
    result_events: list  # json
    current_knowledge_model: dict  # json
    integrity_ok: bool = True

    def get_id(self):
        return self.branch_uuid

    def query_vars(self):
        return (
            self.branch_uuid,
            self.metamodel_version,
            _wrap_json(self.migration_state),
            self.branch_previous_package_id,
            self.target_package_id,
            _wrap_json(self.branch_events),
            _wrap_json(self.target_package_events),
            _wrap_json(self.result_events),
            _wrap_json(self.current_knowledge_model),
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return KMMigration(
            branch_uuid=doc['branchUuid'],
            metamodel_version=doc['metamodelVersion'],
            migration_state=doc['migrationState'],
            branch_previous_package_id=doc['branchPreviousPackageId'],
            target_package_id=doc['targetPackageId'],
            branch_events=doc['branchEvents'],
            target_package_events=doc['targetPackageEvents'],
            result_events=doc['resultEvents'],
            current_knowledge_model=doc['currentKnowledgeModel'],
        )


@dataclasses.dataclass
class Level:
    COLLECTION = 'levels'
    TABLE_NAME = 'level'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'level',
            'title',
            'description',
            'created_at',
            'updated_at',
        ]
    )

    level: int  # integer PK
    title: str  # varchar
    description: str  # varchar
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.level

    def query_vars(self):
        return (
            self.level,
            self.title,
            self.description,
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Level(
            level=doc['level'],
            title=doc['title'],
            description=doc['description'],
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class Metric:
    COLLECTION = 'metrics'
    TABLE_NAME = 'metric'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'title',
            'abbreviation',
            'description',
            'reference_json',
            'created_at',
            'updated_at',
        ]
    )

    uuid: int  # uuid PK
    title: str  # varchar
    abbreviation: str  # varchar
    description: str  # varchar
    reference_json: dict  # json
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.title,
            self.abbreviation,
            self.description,
            _wrap_json(self.reference_json),
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Metric(
            uuid=doc['uuid'],
            title=doc['title'],
            abbreviation=doc['abbreviation'],
            description=doc['description'],
            reference_json=doc['references'],
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class Package:
    COLLECTION = 'packages'
    TABLE_NAME = 'package'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
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

    id: str  # varchar PK
    name: str  # varchar
    organization_id: str  # varchar
    km_id: str  # varchar
    version: str  # varchar
    metamodel_version: int  # integer
    description: str  # varchar
    readme: str  # varchar
    license: str  # varchar
    previous_package_id: str  # varchar FK
    fork_of_package_id: str  # varchar FK
    merge_checkpoint_package_id: str  # varchar FK
    events: list  # json
    created_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.id

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
            _wrap_json(self.events),
            self.created_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Package(
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
            created_at=doc.get('createdAt', now),
        )


@dataclasses.dataclass
class QuestionnaireMigration:
    COLLECTION = 'questionnaireMigrations'
    TABLE_NAME = 'questionnaire_migration'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'old_questionnaire_uuid',
            'new_questionnaire_uuid',
            'resolved_question_uuids',
        ]
    )

    old_questionnaire_uuid: str  # uuid PK FK??
    new_questionnaire_uuid: str  # uuid PK FK??
    resolved_question_uuids: list  # json
    integrity_ok: bool = True

    def get_id(self):
        return f'{self.old_questionnaire_uuid}_{self.new_questionnaire_uuid}'

    def query_vars(self):
        return (
            self.old_questionnaire_uuid,
            self.new_questionnaire_uuid,
            _wrap_json(self.resolved_question_uuids),
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return QuestionnaireMigration(
            old_questionnaire_uuid=doc['oldQuestionnaireUuid'],
            new_questionnaire_uuid=doc['newQuestionnaireUuid'],
            resolved_question_uuids=doc.get('resolvedQuestionUuids', []),
        )


@dataclasses.dataclass
class Questionnaire:
    COLLECTION = 'questionnaires'
    TABLE_NAME = 'questionnaire'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'name',
            'visibility',
            'sharing',
            'package_id',
            'selected_tag_uuids',
            'template_id',
            'format_uuid',
            'creator_uuid',
            'events',
            'versions',
            'created_at',
            'updated_at',
        ]
    )

    uuid: str  # uuid PK
    name: str  # varchar
    visibility: str  # varchar
    sharing: str  # varchar
    package_id: str  # varchar FK??
    selected_tag_uuids: list  # json
    template_id: str  # varchar FK??
    format_uuid: str  # uuid
    creator_uuid: str  # uuid FK??
    events: list  # json
    versions: list  # json
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.name,
            self.visibility,
            self.sharing,
            self.package_id,
            _wrap_json(self.selected_tag_uuids),
            self.template_id,
            self.format_uuid,
            self.creator_uuid,
            _wrap_json(self.events),
            _wrap_json(self.versions),
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Questionnaire(
            uuid=doc['uuid'],
            name=doc['name'],
            visibility=doc['visibility'],
            sharing=doc['sharing'],
            package_id=doc['packageId'],
            selected_tag_uuids=doc.get('selectedTagUuids', []),
            template_id=doc.get('templateId', None),
            format_uuid=doc.get('formatUuid', None),
            creator_uuid=doc.get('creatorUuid', None),
            events=doc.get('events', []),
            versions=doc.get('versions', []),
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class QuestionnaireACLGroup:
    TABLE_NAME = 'questionnaire_acl_group'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'group_id',
            'perms',
            'questionnaire_uuid',
        ]
    )

    uuid: str  # uuid PK
    group_id: str  # uuid FK
    perms: list  # text[]
    questionnaire_uuid: str  # uuid FK
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.group_id,
            self.perms,
            self.questionnaire_uuid,
        )


@dataclasses.dataclass
class QuestionnaireACLUser:
    TABLE_NAME = 'questionnaire_acl_user'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'user_uuid',
            'perms',
            'questionnaire_uuid',
        ]
    )

    uuid: str  # uuid PK
    user_uuid: str  # uuid FK
    perms: list  # text[]
    questionnaire_uuid: str  # uuid FK
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.user_uuid,
            self.perms,
            self.questionnaire_uuid,
        )

    @staticmethod
    def from_mongo(doc: dict, permission: dict, now: datetime.datetime):
        return QuestionnaireACLUser(
            uuid=str(uuid.uuid4()),
            user_uuid=permission['member']['uuid'],
            perms=permission['perms'],
            questionnaire_uuid=doc['uuid'],
        )


@dataclasses.dataclass
class Template:
    COLLECTION = 'templates'
    TABLE_NAME = 'template'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
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
            'created_at',
        ]
    )

    id: str  # varchar PK
    name: str  # varchar
    organization_id: str  # varchar
    template_id: str  # varchar
    version: str  # varchar
    metamodel_version: int  # integer
    description: str  # varchar
    readme: str  # varchar
    license: str  # varchar
    allowed_packages: list  # json
    recommended_package_id: str  # varchar
    formats: list  # json
    created_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.id

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
            _wrap_json(self.allowed_packages),
            self.recommended_package_id,
            _wrap_json(self.formats),
            self.created_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return Template(
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
            created_at=doc.get('createdAt', now),
        )


@dataclasses.dataclass
class TemplateAsset:
    TABLE_NAME = 'template_asset'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'template_id',
            'uuid',
            'file_name',
            'content_type',
        ]
    )

    template_id: str  # varchar FK
    uuid: str  # uuid PK
    file_name: str  # varchar
    content_type: str  # varchar
    original_uuid: str
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.template_id,
            self.uuid,
            self.file_name,
            self.content_type,
        )

    @staticmethod
    def from_mongo(doc: dict, asset: dict, now: datetime.datetime):
        return TemplateAsset(
            template_id=doc['id'],
            uuid=str(uuid.uuid4()),
            file_name=asset['fileName'],
            content_type=asset['contentType'],
            original_uuid=asset['uuid'],
        )


@dataclasses.dataclass
class TemplateFile:
    TABLE_NAME = 'template_file'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'template_id',
            'uuid',
            'file_name',
            'content',
        ]
    )

    template_id: str  # varchar FK
    uuid: str  # uuid PK
    file_name: str  # varchar
    content: str  # varchar
    original_uuid: str
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.template_id,
            self.uuid,
            self.file_name,
            self.content,
        )

    @staticmethod
    def from_mongo(doc: dict, file: dict, now: datetime.datetime):
        return TemplateFile(
            template_id=doc['id'],
            uuid=str(uuid.uuid4()),
            file_name=file['fileName'],
            content=file['content'],
            original_uuid=file['uuid'],
        )


@dataclasses.dataclass
class User:
    COLLECTION = 'users'
    TABLE_NAME = 'user_entity'
    INSERT_QUERY = insert_query(
        table_name=TABLE_NAME,
        fields=[
            'uuid',
            'first_name',
            'last_name',
            'email',
            'password_hash',
            'affiliation',
            'sources',
            'role',
            'permissions',
            'active',
            'submissions_props',
            'image_url',
            'groups',
            'last_visited_at',
            'created_at',
            'updated_at',
        ]
    )

    uuid: str  # uuid PK
    first_name: str  # varchar
    last_name: str  # varchar
    email: str  # varchar
    password_hash: str  # varchar
    affiliation: str  # varchar
    sources: list  # json
    role: str  # varchar
    permissions: list  # text[]
    active: bool  # boolean
    submissions_props: list  # json
    image_url: str  # varchar
    groups: list  # json
    last_visited_at: datetime.datetime  # timestamp+tz
    created_at: datetime.datetime  # timestamp+tz
    updated_at: datetime.datetime  # timestamp+tz
    integrity_ok: bool = True

    def get_id(self):
        return self.uuid

    def query_vars(self):
        return (
            self.uuid,
            self.first_name,
            self.last_name,
            self.email,
            self.password_hash,
            self.affiliation,
            _wrap_json(self.sources),
            self.role,
            self.permissions,
            self.active,
            _wrap_json(self.submissions_props),
            self.image_url,
            _wrap_json(self.groups),
            self.last_visited_at,
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def from_mongo(doc: dict, now: datetime.datetime):
        return User(
            uuid=doc['uuid'],
            first_name=doc['firstName'],
            last_name=doc['lastName'],
            email=doc['email'],
            password_hash=doc['passwordHash'],
            affiliation=doc.get('affiliation', None),
            sources=doc['sources'],
            role=doc['role'],
            permissions=doc['permissions'],
            active=doc.get('active', False),
            submissions_props=doc.get('submissionProps', []),
            image_url=doc.get('imageUrl', None),
            groups=doc.get('groups', []),
            last_visited_at=doc.get('lastVisitedAt', now),
            created_at=doc.get('createdAt', now),
            updated_at=doc.get('updatedAt', now),
        )


@dataclasses.dataclass
class WizardEntities:
    acl_groups: List[ACLGroup] = dataclasses.field(default_factory=list)
    action_keys: List[ActionKey] = dataclasses.field(default_factory=list)
    app_configs: List[AppConfig] = dataclasses.field(default_factory=list)
    book_references: List[BookReference] = dataclasses.field(default_factory=list)
    branches: List[Branch] = dataclasses.field(default_factory=list)
    documents: List[Document] = dataclasses.field(default_factory=list)
    feedbacks: List[Feedback] = dataclasses.field(default_factory=list)
    km_migrations: List[KMMigration] = dataclasses.field(default_factory=list)
    levels: List[Level] = dataclasses.field(default_factory=list)
    metrics: List[Metric] = dataclasses.field(default_factory=list)
    packages: List[Package] = dataclasses.field(default_factory=list)
    questionnaire_migrations: List[QuestionnaireMigration] = dataclasses.field(default_factory=list)
    questionnaires: List[Questionnaire] = dataclasses.field(default_factory=list)
    questionnaire_acl_groups: List[QuestionnaireACLGroup] = dataclasses.field(default_factory=list)
    questionnaire_acl_users: List[QuestionnaireACLUser] = dataclasses.field(default_factory=list)
    templates: List[Template] = dataclasses.field(default_factory=list)
    template_assets: List[TemplateAsset] = dataclasses.field(default_factory=list)
    template_files: List[TemplateFile] = dataclasses.field(default_factory=list)
    users: List[User] = dataclasses.field(default_factory=list)

    _result: List[str] = dataclasses.field(default_factory=list)

    LIST_ENTITY = {
        'acl_groups': ACLGroup,
        'action_keys': ActionKey,
        'app_configs': AppConfig,
        'book_references': BookReference,
        'branches': Branch,
        'documents': Document,
        'feedbacks': Feedback,
        'km_migrations': KMMigration,
        'levels': Level,
        'metrics': Metric,
        'packages': Package,
        'questionnaire_migrations': QuestionnaireMigration,
        'questionnaires': Questionnaire,
        'questionnaire_acl_groups': QuestionnaireACLGroup,
        'questionnaire_acl_users': QuestionnaireACLUser,
        'templates': Template,
        'template_assets': TemplateAsset,
        'template_files': TemplateFile,
        'users': User,
    }
    ENTITIES = [e for e in LIST_ENTITY.values()]
    ENTITY_LIST = {e.__name__: lst for lst, e in LIST_ENTITY.items()}

    def list_by_entity(self, entity) -> list:
        return getattr(self, self.ENTITY_LIST.get(entity.__name__, 'unknown'))

    def set_list(self, entity, lst: list):
        return setattr(self, self.ENTITY_LIST.get(entity.__name__, 'unknown'), lst)

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
        user_ids = self._make_ids_set(User)
        package_ids = self._make_ids_set(Package)
        template_ids = self._make_ids_set(Template)
        questionnaire_ids = self._make_ids_set(Questionnaire)
        branch_ids = self._make_ids_set(Branch)
        group_ids = self._make_ids_set(ACLGroup)

        LOGGER.debug('  - checking references')
        self._check_reference(ActionKey, User, 'user_id', user_ids)
        self._check_optional_reference(Branch, Package, 'previous_package_id', package_ids)
        self._check_reference(Branch, User, 'owner_uuid', user_ids)
        self._check_reference(Document, Questionnaire, 'questionnaire_uuid', questionnaire_ids)
        self._check_reference(Document, Template, 'template_id', template_ids)
        self._check_reference(Document, User, 'creator_uuid', user_ids)
        self._check_reference(Feedback, Package, 'package_id', package_ids)
        self._check_reference(KMMigration, Package, 'branch_previous_package_id', package_ids)
        self._check_reference(KMMigration, Package, 'target_package_id', package_ids)
        self._check_reference(KMMigration, Branch, 'branch_uuid', branch_ids)
        self._check_optional_reference(Package, Package, 'previous_package_id', package_ids)
        self._check_optional_reference(Package, Package, 'fork_of_package_id', package_ids)
        self._check_optional_reference(Package, Package, 'merge_checkpoint_package_id', package_ids)
        self._check_reference(Questionnaire, Package, 'package_id', package_ids)
        self._check_optional_reference(Questionnaire, Template, 'template_id', template_ids)
        self._check_optional_reference(Questionnaire, User, 'creator_uuid', user_ids)
        self._check_reference(QuestionnaireACLGroup, ACLGroup, 'group_id', group_ids)
        self._check_reference(QuestionnaireACLGroup, Questionnaire, 'questionnaire_uuid', questionnaire_ids)
        self._check_reference(QuestionnaireACLUser, Questionnaire, 'questionnaire_uuid', questionnaire_ids)
        self._check_reference(QuestionnaireACLUser, User, 'user_uuid', user_ids)
        self._check_reference(TemplateAsset, Template, 'template_id', template_ids)
        self._check_reference(TemplateFile, Template, 'template_id', template_ids)

        LOGGER.debug(f'  - {len(self._result) - prev_len} new inconsistencies found')
        return prev_len != len(self._result)

    def check_integrity(self) -> List[str]:
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
