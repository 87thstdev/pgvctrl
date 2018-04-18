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
    VersionedDbExceptionRepoExits,
    VersionedDbExceptionRepoEnvDoesNotExits,
    VersionedDbExceptionBadConfigVersionFound)
from dbversioning.osUtil import ensure_dir_exists

Version_Table = namedtuple("version_table", ["tbl", "v", "rev", "hash", "repo", "is_prod", "env"])

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
REVISION = 'revision'
REPOSITORY = 'repository'
REPOSITORIES = 'repositories'
VERSION_STORAGE = 'versionStorage'
VERSION_HASH = "versionHash"
IS_PRODUCTION = "isProduction"
ENV = "env"
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
            ENVS: {},
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
                REVISION: 'revision',
                REPOSITORY: 'repository_name',
                VERSION_HASH: 'version_hash',
                IS_PRODUCTION: 'is_production',
                ENV: 'env'
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
        return {
            TABLE: d[TABLE],
            VERSION: d[VERSION],
            REVISION: d[REVISION],
            VERSION_HASH: d[VERSION_HASH],
            REPOSITORY: d[REPOSITORY],
            IS_PRODUCTION: d[IS_PRODUCTION],
            ENV: d[ENV]
        }

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
                rev=vs[REVISION],
                hash=vs[VERSION_HASH],
                repo=vs[REPOSITORY],
                is_prod=vs[IS_PRODUCTION],
                env=vs[ENV]
            )
            cust_repos.append({
                NAME: repo[NAME],
                VERSION_STORAGE: ver_tbl,
                ENVS: repo[ENVS]
            })

        return cust_repos

    @staticmethod
    def get_repo(repo_name):
        rtn_val = None
        if not repo_name:
            raise VersionedDbExceptionInvalidRepo()

        repo_list = RepositoryConf.repo_list()

        rp = [r for r in repo_list if r[NAME] == repo_name]

        if len(rp) == 1:
            rtn_val = rp[0]
        elif len(rp) > 1:
            raise VersionedDbExceptionBadConfigVersionFound()

        return rtn_val

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
    def remove_repo(repo_name):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    if conf[REPOSITORIES] is None:
                        conf[REPOSITORIES] = list()
                    repos = conf[REPOSITORIES]

                    rp = [r for r in repos if r[NAME] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    conf[REPOSITORIES].remove(rp[0])

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

                    if rp[0][ENVS] is None:
                        rp[0][ENVS] = {}

                    if env not in rp[0][ENVS]:
                        rp[0][ENVS].update({env: None})
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

    @staticmethod
    def remove_repo_env(repo_name, env):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES]
                    rp = [r for r in repos if r[NAME] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][ENVS] is None:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(repo_name, env)

                    del rp[0][ENVS][env]

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

    @staticmethod
    def set_repo_env(repo_name, env, version):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES]
                    rp = [r for r in repos if r[NAME] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][ENVS] and env in rp[0][ENVS]:
                        rp[0][ENVS][env] = version
                    else:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(repo_name, env)

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

    @staticmethod
    def get_repo_env(repo_name, env):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES]
                    rp = [r for r in repos if r[NAME] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][ENVS] is None:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(repo_name, env)

                    if rp[0][ENVS] and env in rp[0][ENVS]:
                        return rp[0][ENVS][env]
                    else:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(repo_name, env)

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

        else:
            raise VersionedDbExceptionFileMissing(RepositoryConf.config_file_name())

        return None
