import os
from datetime import datetime

import requests
import six

from django.core.files.base import ContentFile

from corportal.utils.urls import construct_url

HEADERS_DATETIME_PATTERN = '%a, %d %b %Y %H:%M:%S GMT'


def filepath_generator(path):
    for path, dirs, files in os.walk(path):
        for file_ in files:
            yield os.path.join(path, file_)


class WebDavError(Exception):
    def __init__(self, method, status_code, reason, *args, **kwargs):
        msg = 'Method {method} returns status code: {code} and reason: {reason}'.format(
            method=method, code=status_code, reason=reason
        )
        super(Exception, self).__init__(msg, *args, **kwargs)


class WebDavClient(object):
    HEAD = 'HEAD'
    MKDIR = 'MKCOL'
    PUT = 'PUT'
    DELETE = 'DELETE'
    GET = 'GET'
    EXPECTED_CODES = {
        'mkdir': (201, 301, 405),
        'delete': (204,),
        'upload': (200, 201, 204),
        'download': (200,),
        'exists': (200, 301, 404),
        'size': (200, 301),
        'modified_time': (200, 301, 404)
    }
    NOT_FOUND_CODE = 404

    def __init__(self, base_url, base_path, port=None):
        self._session = requests.session()
        self.base_url = base_url
        self.base_path = base_path
        self.port = port

    def _send(self, method, remote_path, expected_codes, **kwargs):
        full_remote_path = self.get_full_path(remote_path)
        url = construct_url(netloc=self.base_url, path=full_remote_path, port=self.port)
        response = self._session.request(method, url, **kwargs)
        if response.status_code not in expected_codes:
            raise WebDavError(method, response.status_code, response.reason)
        return response

    def mkdir(self, remote_path):
        self._send(
            method=self.MKDIR, remote_path=remote_path, expected_codes=self.EXPECTED_CODES['mkdir']
        )

    def delete(self, remote_path):
        try:
            self._send(
                method=self.DELETE,
                remote_path=remote_path,
                expected_codes=self.EXPECTED_CODES['delete']
            )
        except WebDavError:
            pass

    def upload(self, local_path_or_fileobj, remote_path_with_name, mode='rb'):
        if isinstance(local_path_or_fileobj, six.string_types):
            with open(local_path_or_fileobj, mode) as fileobj:
                self._send(method=self.PUT, remote_path=remote_path_with_name,
                           expected_codes=self.EXPECTED_CODES['upload'], data=fileobj)
        else:
            self._send(method=self.PUT, remote_path=remote_path_with_name,
                       expected_codes=self.EXPECTED_CODES['upload'], data=local_path_or_fileobj)
        return remote_path_with_name

    def download(self, local_path_file):
        response = self._send(
            method=self.GET, remote_path=local_path_file,
            expected_codes=self.EXPECTED_CODES['download'], stream=True
        )
        return ContentFile(response.content)

    def exists(self, remote_path):
        response = self._send(
            method=self.HEAD, remote_path=remote_path, expected_codes=self.EXPECTED_CODES['exists']
        )
        return response.status_code != self.NOT_FOUND_CODE

    def size(self, remote_path):
        response = self._send(
            method=self.HEAD, expected_codes=self.EXPECTED_CODES['size'], remote_path=remote_path
        )
        return int(response.headers.get('content-length', 0))

    def modified_time(self, remote_path):
        response = self._send(
            method=self.HEAD, expected_codes=self.EXPECTED_CODES['modified_time'],
            remote_path=remote_path
        )
        last_modified = response.headers.get('last-modified')
        return datetime.strptime(last_modified, HEADERS_DATETIME_PATTERN) if last_modified else None

    def get_full_path(self, relative_path):
        return os.path.join(self.base_path, relative_path.lstrip(os.path.sep))

    def url(self, relative_path):
        full_path = self.get_full_path(relative_path)
        return construct_url(netloc=self.base_url, path=full_path)

    def upload_dir(self, remote_path, local_path):
        local_files = filepath_generator(local_path)
        for local_file in local_files:
            _, relative_path_local_file = local_file.split(local_path)
            _remote_path = os.path.join(remote_path, relative_path_local_file.lstrip(os.path.sep))
            self.upload(local_path_or_fileobj=local_file, remote_path_with_name=_remote_path)
