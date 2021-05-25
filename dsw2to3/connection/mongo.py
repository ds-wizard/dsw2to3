import datetime
import gridfs
import pymongo
import tenacity

from typing import Optional

from dsw2to3.config import MongoConfig
from dsw2to3.errors import ERROR_HANDLER
from dsw2to3.logger import LOGGER

DOCUMENT_FS_COLLECTION = 'documentFs'
ASSETS_FS_COLLECTION = 'templateAssetFs'


def _fetch_file(fs: gridfs.GridFS, file_name: str) -> Optional[bytes]:
    data = None
    try:
        file = fs.find_one({'filename': file_name})
        if file is not None:
            data = file.read()
            file.close()
    except Exception as e:
        LOGGER.debug(f'Failed to retrieve file from GridFS '
                     f'with filename {file_name}: {e}')
    return data


class MongoDB:

    def __init__(self, config: MongoConfig):
        self.config = config
        self.client = pymongo.MongoClient(**self.config.mongo_client_kwargs)
        self.db = self.client[self.config.database]
        self.doc_fs = gridfs.GridFS(self.db, DOCUMENT_FS_COLLECTION)
        self.asset_fs = gridfs.GridFS(self.db, ASSETS_FS_COLLECTION)
        self.now = datetime.datetime.now()

    def update_now(self):
        self.now = datetime.datetime.now()

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.5),
        stop=tenacity.stop_after_attempt(3),
    )
    def load_list(self, entity) -> list:
        result = list()
        for doc in self.db[entity.COLLECTION].find():
            try:
                result.append(entity.from_mongo(doc, self.now))
            except Exception as e:
                ERROR_HANDLER.error(
                    cause='MongoDB',
                    message=f'- cannot load {entity.__name__} '
                            f'({e}): {doc}'
                )
        LOGGER.info(f'- loaded {entity.__name__}: {len(result)} entries')
        return result

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.5),
        stop=tenacity.stop_after_attempt(3),
    )
    def load_nested(self, source_entity, target_entity, field: str):
        result = list()
        for doc in self.db[source_entity.COLLECTION].find():
            children = doc.get(field, [])
            try:
                for child in children:
                    result.append(target_entity.from_mongo(doc, child, self.now))
            except Exception as e:
                ERROR_HANDLER.error(
                    cause='MongoDB',
                    message=f'- cannot load {target_entity.__name__} '
                            f'({e}): {doc}'
                )
        LOGGER.info(f'- loaded {target_entity.__name__}: {len(result)} entries')
        return result

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.5),
        stop=tenacity.stop_after_attempt(3),
    )
    def fetch_document(self, file_name: str) -> Optional[bytes]:
        return _fetch_file(self.doc_fs, file_name)

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.5),
        stop=tenacity.stop_after_attempt(3),
    )
    def fetch_asset(self, file_name: str) -> Optional[bytes]:
        return _fetch_file(self.asset_fs, file_name)

