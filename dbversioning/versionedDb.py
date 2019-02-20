import hashlib
import os
from typing import List

from dbversioning.errorUtil import (
    VersionedDbExceptionRepoVersionNumber,
    VersionedDbExceptionRepoDoesNotExits,
    VersionedDbExceptionRepoNameInvalid,
    VersionedDbExceptionSqlNamingError)
from dbversioning.osUtil import dir_exists
from dbversioning.repositoryconf import (
    DATA_DUMP_DIR,
    ROLLBACK_FILE_ENDING)
from dbversioning.versionedDbHelper import (
    get_valid_elements,
    get_valid_sql_elements)


class SchemaSnapshot(object):
    def __init__(self, repo_path):
        """
        init_db: Initialize a database to use dbvctrl
        """
        self._repo_path = repo_path
        self.db_name = os.path.basename(repo_path)
        self.schema_snapshot_versions = self._populate_schema_snapshot_versions()

    def _populate_schema_snapshot_versions(self):
        ver_list = []

        for v in get_valid_elements(self._repo_path):
            v_path = os.path.join(self._repo_path, v)
            ver_list.append(SchemaSnapshotVersion(v_path))

        return ver_list


class SchemaSnapshotVersion(object):
    def __init__(self, ss_version_path):
        self._version_path = ss_version_path
        self.major = None
        self.minor = None
        self.maintenance = None

        _set_version_info(os.path.basename(self._version_path), self)
        file_array = os.path.splitext(os.path.basename(ss_version_path))

        self.name = file_array[0]

    @property
    def full_name(self):
        return self.name

    @property
    def version_number(self):
        return f"{self.major}.{self.minor}.{self.maintenance}"

    @property
    def sql_file(self):
        return self._version_path


class Version(object):
    def __init__(self, version_path):
        self._version_path = version_path
        self.name = ""
        self.major = None
        self.minor = None
        self.maintenance = None
        self.sql_files = self._set_sql_objs(
            get_valid_sql_elements(self._version_path)
        )

        _set_version_info(os.path.basename(self._version_path), self)

    @property
    def full_name(self):
        if self.name == "":
            return self.version_number

        return f"{self.version_number}.{self.name}"

    @property
    def version_number(self):
        return f"{self.major}.{self.minor}.{self.maintenance}"

    def get_version_hash_set(self):
        file_hashes = []

        for sql in self.sql_files:
            file_hashes.append({"file": sql.fullname, "hash": get_file_hash(file_path=sql.path)})

        return file_hashes

    def _set_sql_objs(self, sql_list):
        sql_objs = []

        for sql in sql_list:
            sql_path = os.path.join(self._version_path, sql)
            sql_objs.append(SqlPatch(sql_path))
        return sorted(sql_objs, key=lambda x: (x.number, x.name))


class VersionDb(object):
    def __init__(self, repo_path):
        """
        init_db: Initialize a database to use dbvctrl
        """
        db_name = os.path.basename(repo_path)
        if not dir_exists(repo_path):
            raise VersionedDbExceptionRepoDoesNotExits(repo_path)

        if db_name == "":
            raise VersionedDbExceptionRepoNameInvalid(db_name)

        self._repo_path = repo_path
        self.db_name = os.path.basename(repo_path)
        self.versions = self._populate_versions()

    def _populate_versions(self) -> List[Version]:
        ver_list = []

        ignored = {DATA_DUMP_DIR}

        ver_locations = get_valid_elements(self._repo_path, ignored)

        for v in ver_locations:
            v_path = os.path.join(self._repo_path, v)
            ver_list.append(Version(v_path))

        return ver_list

    def create_version(self, version):
        os.mkdir(os.path.join(self._repo_path, version))
        return True


def _set_version_info(version_dir, ver):
    ver_array = version_dir.split(".")

    try:
        ver.major = int(ver_array[0])
        ver.minor = int(ver_array[1])
        ver.maintenance = int(ver_array[2])
    except ValueError:
        raise VersionedDbExceptionRepoVersionNumber(version_dir)

    if len(ver_array) > 3:
        ver.name = ver_array[3]


class SqlPatch(object):
    def __init__(self, sql_path):
        file_array = os.path.basename(sql_path).split(".")

        self._path = sql_path

        try:
            self._number = int(file_array[0])
        except ValueError:
            raise VersionedDbExceptionSqlNamingError(sql_path)

        self._name = ".".join(file_array[1:(len(file_array)-1)])

    @property
    def fullname(self):
        return f"{self._number}.{self.name}"

    @property
    def name(self):
        return self._name

    @property
    def number(self):
        return self._number

    @property
    def path(self):
        return self._path

    @property
    def is_rollback(self):
        return self._path.endswith(ROLLBACK_FILE_ENDING)


class GenericSql(object):
    def __init__(self, sql_path):
        file_name = os.path.basename(sql_path)

        self._path = sql_path
        self._name = file_name

    @property
    def fullname(self):
        return self._name

    @property
    def path(self):
        return self._path


def get_file_hash(file_path) -> str:
    buffer_size = 65536
    sha1 = hashlib.sha1()

    with open(file_path, "rb") as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            sha1.update(data)

    return sha1.hexdigest()
