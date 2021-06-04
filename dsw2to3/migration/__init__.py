from dsw2to3.config import Config
from dsw2to3.migration.common import Migrator, MigrationOptions
from dsw2to3.migration.wizard import WizardMigrator
from dsw2to3.migration.registry import RegistryMigrator


class MigratorFactory:

    @staticmethod
    def create(registry: bool, config: Config, options: MigrationOptions) -> Migrator:
        if registry:
            return RegistryMigrator(config=config, options=options)
        return WizardMigrator(config=config, options=options)


__all__ = ['Migrator', 'WizardMigrator', 'MigrationOptions']
