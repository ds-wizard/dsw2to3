import contextlib
import minio
import minio.datatypes
import tempfile
import tenacity

from typing import Iterable

from dsw2to3.config import S3Config


DOCUMENTS_DIR = 'documents'
TEMPLATES_DIR = 'templates'


@contextlib.contextmanager
def temp_binary_file(data: bytes):
    file = tempfile.TemporaryFile()
    file.write(data)
    file.seek(0)
    yield file
    file.close()


class S3Storage:

    @staticmethod
    def _get_endpoint(url: str):
        parts = url.split('://', maxsplit=1)
        return parts[0] if len(parts) == 1 else parts[1]

    def __init__(self, config: S3Config):
        self.config = config
        endpoint = self._get_endpoint(self.config.url)
        self.client = minio.Minio(
            endpoint=endpoint,
            access_key=self.config.username,
            secret_key=self.config.password,
            secure=self.config.url.startswith('https://'),
        )

    @property
    def identification(self) -> str:
        return f'{self.config.url}/{self.config.bucket}'

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.5),
        stop=tenacity.stop_after_attempt(3),
    )
    def bucket_exists(self):
        return self.client.bucket_exists(self.config.bucket)

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.5),
        stop=tenacity.stop_after_attempt(3),
    )
    def ensure_bucket(self):
        found = self.client.bucket_exists(self.config.bucket)
        if not found:
            self.client.make_bucket(self.config.bucket)

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=0.5),
        stop=tenacity.stop_after_attempt(3),
    )
    def _store_file(self, file_name: str, content_type: str, data: bytes):
        with temp_binary_file(data=data) as file:
            self.client.put_object(
                bucket_name=self.config.bucket,
                object_name=file_name,
                data=file,
                length=len(data),
                content_type=content_type,
            )

    def store_document(self, file_name: str, content_type: str, data: bytes):
        self._store_file(
            file_name=f'{DOCUMENTS_DIR}/{file_name}',
            data=data,
            content_type=content_type,
        )

    def store_template_asset(self, template_id: str, file_name: str, content_type: str, data: bytes):
        self._store_file(
            file_name=f'{TEMPLATES_DIR}/{template_id}/{file_name}',
            data=data,
            content_type=content_type,
        )

    def list_documents(self) -> Iterable[minio.datatypes.Object]:
        return self.client.list_objects(
            bucket_name=self.config.bucket,
            prefix=DOCUMENTS_DIR,
        )

    def list_templates(self, recursive=False) -> Iterable[minio.datatypes.Object]:
        return self.client.list_objects(
            bucket_name=self.config.bucket,
            prefix=TEMPLATES_DIR,
            recursive=recursive,
        )

    def count_documents(self) -> int:
        cnt = 0
        for _ in self.list_documents():
            cnt += 1
        return cnt

    def count_templates(self) -> int:
        cnt = 0
        for _ in self.list_templates():
            cnt += 1
        return cnt

    def delete_documents(self):
        for obj in self.list_documents():
            self.client.remove_object(
                bucket_name=obj.bucket_name,
                object_name=obj.object_name
            )

    def delete_templates(self):
        for obj in self.list_templates(recursive=True):
            self.client.remove_object(
                bucket_name=obj.bucket_name,
                object_name=obj.object_name
            )

    def recreate_bucket(self):
        self.client.remove_bucket(bucket_name=self.config.bucket)
        self.ensure_bucket(bucket_name=self.config.bucket)
