import os
import simplejson as json
from simplejson import JSONDecodeError

from collections import namedtuple

from .errorUtil import VersionedDbExceptionBadConfigFile, \
    VersionedDbExceptionFileMissing, \
    VersionedDbExceptionInvalidRepo

Version_Table = namedtuple("version_table", ["tbl", "v", "hash", "repo"])

ROOT = 'root'
HIDDEN = '.'
AUTO_SNAPSHOTS = 'autoSnapshots'
FAST_FORWARD = '_fastForward'
ROLLBACK_FILE_ENDING = '_rollback.sql'
SNAPSHOTS = '_snapshots'
DATA_DUMP = 'data'
DEFAULT_VERSION_STORAGE = 'defaultVersionStorage'
TABLE = 'table'
VERSION = 'version'
REPOSITORY = 'repository'
REPOSITORIES = 'repositories'
VERSION_STORAGE = 'versionStorage'
VERSION_HASH = "versionHash"
NAME = "name"


class RepositoryConf(object):
    _config_json = {
        ROOT: 'databases',
        AUTO_SNAPSHOTS: True,
        DEFAULT_VERSION_STORAGE: {
            TABLE: 'repository_version',
            VERSION: 'version',
            REPOSITORY: 'repository_name',
            VERSION_HASH: 'version_hash'
        }, REPOSITORIES: [
            {
                NAME: '',
                VERSION_STORAGE: {
                    TABLE: '',
                    VERSION: '',
                    REPOSITORY: '',
                    VERSION_HASH: ''
                }
            }
        ]
    }

    _config_name = 'dbRepoConfig.json'

    @staticmethod
    def __init__():
        """"""

    @staticmethod
    def config_file_name():
        return RepositoryConf._config_name

    @staticmethod
    def config_json():
        return RepositoryConf._config_json

    @staticmethod
    def _get_repo_dict():
        d = None

        if os.path.isfile(RepositoryConf._config_name):
            try:
                with open(RepositoryConf._config_name) as json_data:
                    d = json.load(json_data)
            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()
        else:
            raise VersionedDbExceptionFileMissing(RepositoryConf._config_name)

        return d

    @staticmethod
    def root():
        conf = RepositoryConf._get_repo_dict()
        return conf[ROOT]

    @staticmethod
    def auto_snapshots():
        conf = RepositoryConf._get_repo_dict()
        return conf[AUTO_SNAPSHOTS]

    @staticmethod
    def fast_forward_dir():
        return os.path.join(RepositoryConf.root(), FAST_FORWARD)

    @staticmethod
    def get_data_dump_dir(repo_name):
        if not repo_name:
            raise VersionedDbExceptionInvalidRepo()

        return os.path.join(RepositoryConf.root(), repo_name, DATA_DUMP)

    @staticmethod
    def get_data_dump_sql_dir(repo_name, sql_file):
        if not repo_name:
            raise VersionedDbExceptionInvalidRepo()

        return os.path.join(RepositoryConf.root(), repo_name, DATA_DUMP, sql_file)

    @staticmethod
    def snapshot_dir():
        return os.path.join(RepositoryConf.root(), SNAPSHOTS)

    @staticmethod
    def default_version_storage():
        conf = RepositoryConf._get_repo_dict()
        d = conf[DEFAULT_VERSION_STORAGE]
        return Version_Table(
            tbl=d[TABLE],
            v=d[VERSION],
            hash=d[VERSION_HASH],
            repo=d[REPOSITORY]
        )

    @staticmethod
    def custom_repo_list():
        conf = RepositoryConf._get_repo_dict()
        repos = conf[REPOSITORIES]
        cust_repos = []

        for repo in repos:
            vs = repo[VERSION_STORAGE]
            ver_tbl = Version_Table(
                tbl=vs[TABLE],
                v=vs[VERSION],
                hash=vs[VERSION_HASH],
                repo=vs[REPOSITORY]
            )
            cust_repos.append({
                NAME: repo[NAME],
                VERSION_STORAGE: ver_tbl
            })

        return cust_repos

    @staticmethod
    def get_custom_repo(repo_name):
        if not repo_name:
            raise VersionedDbExceptionInvalidRepo()

        repo_list = RepositoryConf.custom_repo_list()

        rp = [r for r in repo_list if r[NAME] == repo_name]

        return rp
