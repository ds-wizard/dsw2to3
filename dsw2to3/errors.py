import logging
import sys

from dsw2to3.logger import LOGGER


class MigrationError(RuntimeError):

    def __init__(self, cause: str, message: str, level: int):
        super().__init__()
        self.cause = cause
        self.message = message
        self.level = level


def _log_handle(error: MigrationError):
    LOGGER.log(level=error.level, msg=f'{error.cause}:  {error.message}')


def _stop_handle(error: MigrationError):
    if error.level >= logging.ERROR:
        LOGGER.critical(msg=error.message)
        LOGGER.info('Exiting... you can try to run with --best-effort flag')
        sys.exit(1)
    else:
        _log_handle(error=error)


class MigrationErrorHandler:

    def __init__(self):
        self._handler = _stop_handle

    def set_log(self):
        self._handler = _log_handle

    def set_stop(self):
        self._handler = _stop_handle

    def warning(self, cause: str, message: str):
        self._handler(MigrationError(cause=cause, message=message, level=logging.WARNING))

    def error(self, cause: str, message: str):
        self._handler(MigrationError(cause=cause, message=message, level=logging.ERROR))

    def critical(self, cause: str, message: str):
        self._handler(MigrationError(cause=cause, message=message, level=logging.CRITICAL))


ERROR_HANDLER = MigrationErrorHandler()
