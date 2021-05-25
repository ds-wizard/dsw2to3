import click

from typing import TextIO

from dsw2to3.config import Config, ConfigParser, MissingConfigurationError
from dsw2to3.consts import PROG_NAME, VERSION
from dsw2to3.errors import ERROR_HANDLER
from dsw2to3.logger import LOGGER
from dsw2to3.migration import MigratorFactory, MigrationOptions

LOG_LEVELS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
DEFAULT_LOG_LEVEL = 'INFO'


def validate_config(ctx, param, value: TextIO) -> Config:
    content = value.read()
    cfg_parser = ConfigParser()

    if not cfg_parser.can_read(content):
        click.echo('Error: Cannot parse config file', err=True)
        exit(1)

    try:
        cfg_parser.read_string(content)
        cfg_parser.validate()
        return cfg_parser.config
    except MissingConfigurationError as e:
        click.echo('Error: Missing configuration', err=True)
        for missing_item in e.missing:
            click.echo(f' - {missing_item}')
        exit(1)


@click.command(name=PROG_NAME)
@click.option('-c', '--config', type=click.File('r'),
              callback=validate_config, required=True,
              help='Config YML file (see example).')
@click.option('-b', '--best-effort', is_flag=True,
              help='Run in best effort mode (continue on errors, not recommended).')
@click.option('-r', '--registry', is_flag=True,
              help='Migrate DSW Registry instead of Wizard.')
@click.option('-d', '--dry-run', is_flag=True,
              help='Run without making any changes in the migration targets.')
@click.option('-f', '--fix-integrity', is_flag=True,
              help='Fix integrity by skipping invalid or duplicate entries.')
@click.option('-s', '--skip-pre-check', is_flag=True,
              help='Skip initial checks of existing data in databases.')
@click.option('-l', '--log-level', type=click.Choice(LOG_LEVELS),
              default=DEFAULT_LOG_LEVEL, show_default=True,
              help='Set logging level.')
@click.version_option(version=VERSION, prog_name=PROG_NAME)
def main(config: Config, dry_run: bool, best_effort: bool, skip_pre_check: bool,
         fix_integrity: bool, registry: bool, log_level: str):
    """CLI tool to support data migration from DSW 2.14 to DSW 3.0"""
    LOGGER.setLevel(log_level)
    if best_effort:
        ERROR_HANDLER.set_log()
    options = MigrationOptions(
        dry_run=dry_run,
        fix_integrity=fix_integrity,
        skip_pre_check=skip_pre_check,
    )
    migrator = MigratorFactory.create(
        registry=registry,
        config=config,
        options=options,
    )
    migrator.migrate()
    migrator.finish()
