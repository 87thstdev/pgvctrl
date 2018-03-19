import os
import simplejson as json
from simplejson import JSONDecodeError

from collections import namedtuple

from dbversioning.errorUtil import (
    VersionedDbExceptionBadConfigFile,
    VersionedDbExceptionFileMissing,
    VersionedDbExceptionInvalidRepo,
    VersionedDbExceptionRepoEnvExits,
    VersionedDbExceptionRepoDoesNotExits,
    VersionedDbExceptionRepoExits)
from dbversioning.osUtil import ensure_dir_exists

Version_Table = namedtuple("version_table", ["tbl", "v", "hash", "repo", "is_prod"])

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
IS_PRODUCTION = "isProduction"
NAME = "name"
ENVS = "envs"


class RepositoryConf(object):

    @staticmethod
    def __init__():
        """"""

    @staticmethod
    def repo_conf(repo_name=None):
        repo_name_val = repo_name
        if not repo_name_val:
            repo_name_val = ''

        return {
            ENVS: [],
            NAME: repo_name_val,
            VERSION_STORAGE: RepositoryConf.default_version_storage()
        }

    @staticmethod
    def config_file_name():
        return 'dbRepoConfig.json'

    @staticmethod
    def config_json():
        config_json = {
            ROOT: 'databases',
            AUTO_SNAPSHOTS: True,
            DEFAULT_VERSION_STORAGE: {
                TABLE: 'repository_version',
                VERSION: 'version',
                REPOSITORY: 'repository_name',
                VERSION_HASH: 'version_hash',
                IS_PRODUCTION: 'is_production'
            }, REPOSITORIES: []
        }
        return config_json

    @staticmethod
    def _get_repo_dict():
        d = None

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()) as json_data:
                    d = json.load(json_data)
            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()
        else:
            raise VersionedDbExceptionFileMissing(RepositoryConf.config_file_name)

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
            repo=d[REPOSITORY],
            is_prod=d[IS_PRODUCTION]
        )

    @staticmethod
    def repo_list():
        conf = RepositoryConf._get_repo_dict()
        repos = conf[REPOSITORIES]
        cust_repos = []

        for repo in repos:
            vs = repo[VERSION_STORAGE]
            ver_tbl = Version_Table(
                tbl=vs[TABLE],
                v=vs[VERSION],
                hash=vs[VERSION_HASH],
                repo=vs[REPOSITORY],
                is_prod=vs[IS_PRODUCTION]
            )
            cust_repos.append({
                NAME: repo[NAME],
                VERSION_STORAGE: ver_tbl
            })

        return cust_repos

    @staticmethod
    def get_repo(repo_name):
        if not repo_name:
            raise VersionedDbExceptionInvalidRepo()

        repo_list = RepositoryConf.repo_list()

        rp = [r for r in repo_list if r[NAME] == repo_name]

        return rp

    @staticmethod
    def create_repo(repo_name):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    if conf[REPOSITORIES] is None:
                        conf[REPOSITORIES] = list()
                    repos = conf[REPOSITORIES]

                    rp = [r for r in repos if r[NAME] == repo_name]

                    if rp:
                        raise VersionedDbExceptionRepoExits(repo_name)

                    conf[REPOSITORIES].append(RepositoryConf.repo_conf(repo_name))

                    out_str = json.dumps(
                            conf,
                            indent=4,
                            sort_keys=True,
                            separators=(',', ': '),
                            ensure_ascii=True)

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), 'w') as outfile:
                outfile.write(out_str)

            ensure_dir_exists(os.path.join(RepositoryConf.root(), repo_name))
        else:
            raise VersionedDbExceptionFileMissing(RepositoryConf.config_file_name())

        return True

    @staticmethod
    def create_repo_env(repo_name, env):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES]
                    rp = [r for r in repos if r[NAME] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    env_list = list()

                    if rp[0][ENVS] is None:
                        rp[0][ENVS] = list()

                    for e in rp[0][ENVS]:
                        if list(e.keys()):
                            env_list.append(list(e.keys())[0])

                    if env not in env_list:
                        rp[0][ENVS].append({env: None})
                    else:
                        raise VersionedDbExceptionRepoEnvExits(repo_name, env)

                    out_str = json.dumps(
                            conf,
                            indent=4,
                            sort_keys=True,
                            separators=(',', ': '),
                            ensure_ascii=True)

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), 'w') as outfile:
                outfile.write(out_str)
        else:
            raise VersionedDbExceptionFileMissing(RepositoryConf.config_file_name())

        return True
