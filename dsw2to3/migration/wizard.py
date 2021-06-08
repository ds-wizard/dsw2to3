from typing import List

from dsw2to3.config import Config
from dsw2to3.connection import PostgresDB, MongoDB, S3Storage
from dsw2to3.errors import ERROR_HANDLER
from dsw2to3.migration.common import Migrator, MigrationOptions
from dsw2to3.migration.entitites import *


class WizardMigrator(Migrator):

    # All tables but in specific order (bcs FK)
    _TABLES_CLEANUP = [
        Level.TABLE_NAME,
        Metric.TABLE_NAME,
        ActionKey.TABLE_NAME,
        AppConfig.TABLE_NAME,
        BookReference.TABLE_NAME,
        Branch.TABLE_NAME,
        'document_queue',
        Feedback.TABLE_NAME,
        KMMigration.TABLE_NAME,
        QuestionnaireMigration.TABLE_NAME,
        QuestionnaireACLGroup.TABLE_NAME,
        QuestionnaireACLUser.TABLE_NAME,
        ACLGroup.TABLE_NAME,
        TemplateAsset.TABLE_NAME,
        TemplateFile.TABLE_NAME,
        Document.TABLE_NAME,
        Questionnaire.TABLE_NAME,
        Template.TABLE_NAME,
        Package.TABLE_NAME,
        User.TABLE_NAME,
    ]

    _LOAD_ENTITIES_SIMPLE = [
        ActionKey,
        AppConfig,
        BookReference,
        Branch,
        Document,
        Feedback,
        KMMigration,
        Level,
        Metric,
        QuestionnaireMigration,
        Questionnaire,
        Package,
        Template,
        User,
    ]

    _LOAD_ENTITIES_NESTED = [
        (QuestionnaireACLUser, Questionnaire, 'permissions'),
        (TemplateAsset, Template, 'assets'),
        (TemplateFile, Template, 'files'),
    ]

    _DEFAULT_TABLE_COUNTS = {
        ACLGroup.TABLE_NAME: 0,
        ActionKey.TABLE_NAME: 0,
        AppConfig.TABLE_NAME: 1,  # default app config
        BookReference.TABLE_NAME: 0,
        Branch.TABLE_NAME: 0,
        Document.TABLE_NAME: 0,
        'document_queue': 0,
        Feedback.TABLE_NAME: 0,
        KMMigration.TABLE_NAME: 0,
        Level.TABLE_NAME: 3,  # default levels (proposal, DMP, project)
        Metric.TABLE_NAME: 6,  # default metrics (F,A,I,R,G,O)
        Package.TABLE_NAME: 0,
        QuestionnaireMigration.TABLE_NAME: 0,
        Questionnaire.TABLE_NAME: 0,
        QuestionnaireACLGroup.TABLE_NAME: 0,
        QuestionnaireACLUser.TABLE_NAME: 0,
        Template.TABLE_NAME: 0,
        TemplateAsset.TABLE_NAME: 0,
        TemplateFile.TABLE_NAME: 0,
        User.TABLE_NAME: 3,  # default users (albert, nikola, isaac)
    }

    _INSERT_ARGS = [
        (User, False),  # User (FKs: User)
        (Package, True),  # Package (FKs: Package)
        (Template, False),  # Template (FKs: -)
        (Questionnaire, False),  # Questionnaire (FKs: Package, Template, User)
        (ActionKey, False),  # ActionKey (FKs: User)
        (AppConfig, False),  # AppConfig (FKs: -)
        (BookReference, False),  # BookReference (FKs: -)
        (Branch, False),  # Branch (FKs: Package, User)
        (Document, False),  # Document (FKs: Questionnaire, Template, User)
        (Feedback, False),  # Feedback (FKs: Package)
        (KMMigration, False),  # KMMigration (FKs: Branch, Package)
        (Level, False),  # Level (FKs: -)
        (Metric, False),  # Metric (FKs: -)
        (QuestionnaireMigration, False),  # QuestionnaireMigration (FKs: Questionnaire)
        (QuestionnaireACLUser, False),  # QuestionnaireACLUser (FKs: Questionnaire, User)
        (TemplateAsset, False),  # TemplateAsset (FKs: Template)
        (TemplateFile, False),  # TemplateFile (FKs: Template)
    ]

    def __init__(self, config: Config, options: MigrationOptions):
        super().__init__(config, options)
        self.entities = WizardEntities()
        self.postgres = PostgresDB(config=config.postgres)
        self.mongo = MongoDB(config=config.mongo)
        self.s3 = S3Storage(config=config.s3)

    def pre_check(self) -> bool:
        all_ok = True
        LOGGER.info(f'Checking source database (MongoDB)')
        try:
            source_migrations = self.mongo.db['migrations'].count_documents({})
            expect_migrations = 42  # There are 42 DB migrations in DSW 2.14.0
            if source_migrations != expect_migrations:
                all_ok = False
                ERROR_HANDLER.error(
                    cause='MongoDB',
                    message=f'- source database has incorrect version '
                            f'(has {source_migrations} migrations, '
                            f'expected {expect_migrations})',
                )
            else:
                LOGGER.info('- source database seems to has correct version')
        except Exception as e:
            all_ok = False
            ERROR_HANDLER.critical(
                cause='MongoDB',
                message=f'- failed to check data in MongoDB ({e})',
            )
        LOGGER.info(f'Checking target database (PostgreSQL)')
        try:
            with self.postgres.new_cursor() as cursor:
                cursor.execute(query='SELECT * FROM migration;')
                result = cursor.fetchall()
            if len(result) != 1:  # There is 1 DB migration in DSW 3.0.0
                all_ok = False
                ERROR_HANDLER.error(
                    cause='PostgreSQL',
                    message=f'- target database has incorrect version '
                            f'(has {len(result)} migrations, expected 1)',
                )
            else:
                LOGGER.info('- target database seems to has correct version')
            for table, count in self._DEFAULT_TABLE_COUNTS.items():
                try:
                    with self.postgres.new_cursor() as cursor:
                        cursor.execute(
                            query=f'SELECT count(*) FROM {table};',
                        )
                        result = cursor.fetchone()[0]
                        if result > count:
                            all_ok = False
                            ERROR_HANDLER.warning(
                                cause='PostgreSQL',
                                message=f'- there are some custom data in table {table}',
                            )
                except Exception as e:
                    all_ok = False
                    ERROR_HANDLER.critical(
                        cause='PostgreSQL',
                        message=f'- failed to check table "{table}" ({e})',
                    )
        except Exception as e:
            all_ok = False
            ERROR_HANDLER.critical(
                cause='PostgreSQL',
                message=f'- failed to check data in PostgreSQL ({e})',
            )
        LOGGER.info(f'Checking target file storage (S3)')
        try:
            if self.s3.bucket_exists():
                LOGGER.info(f'- S3 storage seems to be OK (bucket exists)')
            else:
                if not self._dry_run_tag():
                    self.s3.ensure_bucket()
                LOGGER.info(f'- S3 storage seems to be OK (bucket created)'
                            f'{self._dry_run_tag()}')
        except Exception as e:
            all_ok = False
            ERROR_HANDLER.critical(
                cause='S3',
                message=f'- failed to check data in S3 storage ({e})',
            )
        return all_ok

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
            documents = self.s3.count_documents()
            templates = self.s3.count_templates()
            LOGGER.debug(f'- there are {documents} documents and {templates} '
                         f'templates in the S3 bucket')
            if documents > 0 or templates > 0:
                LOGGER.info(f'- cleaning S3 bucket (deleting objects){self._dry_run_tag()}')
                if not self.options.dry_run:
                    self.s3.delete_documents()
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

    def migrate_documents(self):
        counter = 0
        not_done = 0
        skipped = 0
        documents = list(self.entities.valid_entries(Document))  # type: List[Document]
        LOGGER.info(f'Migrating documents ({len(documents)}'
                    f' files){self._dry_run_tag()}')
        for document in documents:
            if document.state != 'DoneDocumentState':
                LOGGER.debug(f'- skipping migrating file for document {document.uuid},'
                             f'because of its state ({document.state})')
                not_done += 1
                continue
            data = self.mongo.fetch_document(document.uuid)
            if data is None:
                LOGGER.warning(
                    f' - no data found for document {document.uuid},'
                    f'cannot be transferred to S3 storage - skipping'
                )
                skipped += 1
                continue
            if not self.options.dry_run:
                self.s3.store_document(
                    file_name=document.uuid,
                    content_type=document.metadata.get(
                        'contentType',
                        'application/octet-stream',
                    ),
                    data=data,
                )
            counter += 1
        LOGGER.info(f'- {counter} documents stored in S3 '
                    f'({not_done} not in "done" state, '
                    f'{skipped} not found and skipped)')

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
        if not self.options.skip_pre_check:
            self.pre_check()
        else:
            LOGGER.info('Skipping pre-check phase [--skip-pre-check]')

        LOGGER.info('Starting to clean up migration targets')
        self.cleanup_postgres()
        self.cleanup_s3()
        LOGGER.info('Cleaning up migration targets finished')

        LOGGER.info('Starting Wizard database migration')
        self.load()
        self.check_integrity()
        self.insert()
        LOGGER.info('Wizard database migration finished')

        LOGGER.info('Starting Wizard file storage migration')
        self.migrate_documents()
        self.migrate_template_assets()
        LOGGER.info('Wizard file storage migration finished')
