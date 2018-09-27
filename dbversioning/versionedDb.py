import hashlib
import os
from typing import List

from dbversioning.errorUtil import (
    VersionedDbExceptionFastForwardVersion,
    VersionedDbExceptionRepoVersionNumber,
    VersionedDbExceptionRepoDoesNotExits, VersionedDbExceptionRepoNameInvalid)
from dbversioning.osUtil import dir_exists
from dbversioning.repositoryconf import DATA_DUMP
from dbversioning.versionedDbHelper import (
    get_valid_elements,
    get_valid_sql_elements,
)


class FastForwardDb(object):
    def __init__(self, repo_path):
        """
        init_db: Initialize a database to use dbvctrl
        """
        self._repo_path = repo_path
        self.db_name = os.path.basename(repo_path)
        self.fast_forward_versions = self._populate_fast_forward_versions()

    def _populate_fast_forward_versions(self):
        ver_list = []

        for v in get_valid_elements(self._repo_path):
            v_path = os.path.join(self._repo_path, v)
            ver_list.append(FastForwardVersion(v_path))

        return ver_list


class FastForwardVersion(object):
    def __init__(self, ff_version_path):
        self._version_path = ff_version_path
        self.name = ""
        self.major = None
        self.minor = None
        self.maintenance = None

        _set_version_info(os.path.basename(self._version_path), self)

    @property
    def full_name(self):
        if self.name == "":
            return f"{self.major}.{self.minor}.{self.maintenance}"

        return f"{self.major}.{self.minor}.{self.maintenance}.{self.name}"

    @property
    def version_number(self):
        return f"{self.major}.{self.minor}.{self.maintenance}"

    @property
    def sql_file(self):
        return GeneratorExit(self._version_path)


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
            return f"{self.major}.{self.minor}.{self.maintenance}"

        return f"{self.major}.{self.minor}.{self.maintenance}.{self.name}"

    @property
    def version_number(self):
        return f"{self.major}.{self.minor}.{self.maintenance}"

    def get_version_hash_set(self):
        BUF_SIZE = 65536
        file_hashes = []

        sha1 = hashlib.sha1()

        for sql in self.sql_files:
            with open(sql.path, "rb") as f:
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    sha1.update(data)
            file_hashes.append({"file": sql.fullname, "hash": sha1.hexdigest()})

        return file_hashes

    def _set_sql_objs(self, sql_list):
        sql_objs = []

        for sql in sql_list:
            sql_path = os.path.join(self._version_path, sql)
            sql_objs.append(SqlPatch(sql_path))
        return sorted(sql_objs, key=lambda x: x.number)


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

        ignored = {DATA_DUMP}

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

    if not ver_array:
        raise VersionedDbExceptionFastForwardVersion(version_dir)

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
        self._number = int(file_array[0])
        self._name = file_array[1]

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


class GenericSql(object):
    def __init__(self, sql_path):
        file_name = os.path.basename(sql_path)

        self._path = sql_path
        self._name = file_name

    @property
    def name(self):
        return self._name

    @property
    def fullname(self):
        return self._name

    @property
    def path(self):
        return self._path
