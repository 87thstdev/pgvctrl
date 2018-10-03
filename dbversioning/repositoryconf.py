import os
from typing import List, Dict, Union

import simplejson as json
from simplejson import JSONDecodeError

from collections import namedtuple

from dbversioning.errorUtil import (
    VersionedDbExceptionBadConfigFile,
    VersionedDbExceptionFileMissing,
    VersionedDbExceptionRepoEnvExits,
    VersionedDbExceptionRepoDoesNotExits,
    VersionedDbExceptionRepoExits,
    VersionedDbExceptionRepoEnvDoesNotExits,
    VersionedDbExceptionBadConfigVersionFound,
    VersionedDbExceptionIncludeExcludeSchema,
    VersionedDbException)
from dbversioning.osUtil import ensure_dir_exists

Version_Table = namedtuple(
    "version_table", ["tbl", "v", "rev", "hash", "repo", "is_prod", "env", "owner"]
)

ROOT = "root"
HIDDEN = "."
AUTO_SNAPSHOTS_PROP = "autoSnapshots"
FAST_FORWARD_DIR = "_fastForward"
SNAPSHOTS_DIR = "_snapshots"
DATA_DUMP_DIR = "data"
ROLLBACK_FILE_ENDING = "_rollback.sql"
DEFAULT_VERSION_STORAGE_PROP = "defaultVersionStorage"
REPOSITORY_VERSION = "repository_version"
TABLE_PROP = "table"
VERSION_PROP = "version"
REVISION_PROP = "revision"
REPOSITORY_PROP = "repository"
REPOSITORY_NAME = "repository_name"
REPOSITORIES_PROP = "repositories"
VERSION_STORAGE_PROP = "versionStorage"
VERSION_HASH_PROP = "versionHash"
TABLE_OWNER_PROP = "tableOwner"
IS_PRODUCTION_PROP = "isProduction"
ENV_PROP = "env"
NAME_PROP = "name"
ENVS_PROP = "envs"
INCLUDE_TABLES_PROP = "includeTables"
INCLUDE_SCHEMAS_PROP = "includeSchemas"
EXCLUDE_TABLES_PROP = "excludeTables"
EXCLUDE_SCHEMAS_PROP = "excludeSchemas"


class RepositoryConf(object):
    @staticmethod
    def __init__():
        """"""

    @staticmethod
    def repo_conf(repo_name: str):
        repo_name_val = repo_name

        return {
            ENVS_PROP: {},
            NAME_PROP: repo_name_val,
            VERSION_STORAGE_PROP: RepositoryConf.default_version_storage(),
        }

    @staticmethod
    def config_file_name():
        return "dbRepoConfig.json"

    @staticmethod
    def config_json():
        config_json = {
            ROOT: "databases",
            AUTO_SNAPSHOTS_PROP: True,
            DEFAULT_VERSION_STORAGE_PROP: {
                TABLE_PROP: REPOSITORY_VERSION,
                VERSION_PROP: VERSION_PROP,
                REVISION_PROP: REVISION_PROP,
                REPOSITORY_PROP: REPOSITORY_NAME,
                VERSION_HASH_PROP: "version_hash",
                TABLE_OWNER_PROP: None,
                IS_PRODUCTION_PROP: "is_production",
                ENV_PROP: "env",
            },
            REPOSITORIES_PROP: [],
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
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        return d

    @staticmethod
    def root():
        conf = RepositoryConf._get_repo_dict()
        return conf[ROOT]

    @staticmethod
    def auto_snapshots():
        conf = RepositoryConf._get_repo_dict()
        return conf[AUTO_SNAPSHOTS_PROP]

    @staticmethod
    def fast_forward_dir():
        return os.path.join(RepositoryConf.root(), FAST_FORWARD_DIR)

    @staticmethod
    def get_data_dump_dir(repo_name: str):
        return os.path.join(RepositoryConf.root(), repo_name, DATA_DUMP_DIR)

    @staticmethod
    def get_data_dump_sql_dir(repo_name: str, sql_file):
        return os.path.join(
            RepositoryConf.root(), repo_name, DATA_DUMP_DIR, sql_file
        )

    @staticmethod
    def snapshot_dir():
        return os.path.join(RepositoryConf.root(), SNAPSHOTS_DIR)

    @staticmethod
    def default_version_storage():
        conf = RepositoryConf._get_repo_dict()
        d = conf[DEFAULT_VERSION_STORAGE_PROP]
        return {
            TABLE_PROP: d[TABLE_PROP],
            VERSION_PROP: d[VERSION_PROP],
            REVISION_PROP: d[REVISION_PROP],
            VERSION_HASH_PROP: d[VERSION_HASH_PROP],
            TABLE_OWNER_PROP: d[TABLE_OWNER_PROP],
            REPOSITORY_PROP: d[REPOSITORY_PROP],
            IS_PRODUCTION_PROP: d[IS_PRODUCTION_PROP],
            ENV_PROP: d[ENV_PROP],
        }

    @staticmethod
    def repo_list():
        conf = RepositoryConf._get_repo_dict()
        repos = conf[REPOSITORIES_PROP]
        rtn_repos = []

        for repo in repos:
            vs = repo[VERSION_STORAGE_PROP]
            ver_tbl = Version_Table(
                tbl=vs[TABLE_PROP],
                v=vs[VERSION_PROP],
                rev=vs[REVISION_PROP],
                hash=vs[VERSION_HASH_PROP],
                repo=vs[REPOSITORY_PROP],
                is_prod=vs[IS_PRODUCTION_PROP],
                env=vs[ENV_PROP],
                owner=vs[TABLE_OWNER_PROP],
            )
            repo[VERSION_STORAGE_PROP] = ver_tbl
            rtn_repos.append(repo)

        return rtn_repos

    @staticmethod
    def get_repo(repo_name: str):
        rtn_val = None
        repo_list = RepositoryConf.repo_list()

        rp = [r for r in repo_list if r[NAME_PROP] == repo_name]

        if len(rp) == 1:
            rtn_val = rp[0]
        elif len(rp) > 1:
            raise VersionedDbExceptionBadConfigVersionFound()

        return rtn_val

    @staticmethod
    def create_repo(repo_name: str):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES_PROP]

                    rp = [r for r in repos if r[NAME_PROP] == repo_name]

                    if rp:
                        raise VersionedDbExceptionRepoExits(repo_name)

                    conf[REPOSITORIES_PROP].append(
                        RepositoryConf.repo_conf(repo_name)
                    )

                    out_str = json.dumps(
                        conf,
                        indent=4,
                        sort_keys=True,
                        separators=(",", ": "),
                        ensure_ascii=True,
                    )

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), "w") as outfile:
                outfile.write(out_str)

            ensure_dir_exists(os.path.join(RepositoryConf.root(), repo_name))
        else:
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        return True

    @staticmethod
    def remove_repo(repo_name: str):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    if conf[REPOSITORIES_PROP] is None:
                        conf[REPOSITORIES_PROP] = list()
                    repos = conf[REPOSITORIES_PROP]

                    rp = [r for r in repos if r[NAME_PROP] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    conf[REPOSITORIES_PROP].remove(rp[0])

                    out_str = json.dumps(
                        conf,
                        indent=4,
                        sort_keys=True,
                        separators=(",", ": "),
                        ensure_ascii=True,
                    )

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), "w") as outfile:
                outfile.write(out_str)

            ensure_dir_exists(os.path.join(RepositoryConf.root(), repo_name))
        else:
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        return True

    @staticmethod
    def create_repo_env(repo_name: str, env: str):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES_PROP]
                    rp = [r for r in repos if r[NAME_PROP] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][ENVS_PROP] is None:
                        rp[0][ENVS_PROP] = {}

                    if env not in rp[0][ENVS_PROP]:
                        rp[0][ENVS_PROP].update({env: None})
                    else:
                        raise VersionedDbExceptionRepoEnvExits(repo_name, env)

                    out_str = json.dumps(
                        conf,
                        indent=4,
                        sort_keys=True,
                        separators=(",", ": "),
                        ensure_ascii=True,
                    )

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), "w") as outfile:
                outfile.write(out_str)
        else:
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        return True

    @staticmethod
    def remove_repo_env(repo_name: str, env: str):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES_PROP]
                    rp = [r for r in repos if r[NAME_PROP] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][ENVS_PROP] is None:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(
                            repo_name, env
                        )

                    del rp[0][ENVS_PROP][env]

                    out_str = json.dumps(
                        conf,
                        indent=4,
                        sort_keys=True,
                        separators=(",", ": "),
                        ensure_ascii=True,
                    )

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), "w") as outfile:
                outfile.write(out_str)
        else:
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        return True

    @staticmethod
    def set_repo_version_storage_owner(repo_name: str, owner: str):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES_PROP]
                    rp = [r for r in repos if r[NAME_PROP] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][VERSION_STORAGE_PROP]:
                        rp[0][VERSION_STORAGE_PROP][TABLE_OWNER_PROP] = owner
                    else:
                        raise VersionedDbException(f"{repo_name}: {VERSION_STORAGE_PROP} does not exist.")

                    out_str = json.dumps(
                            conf,
                            indent=4,
                            sort_keys=True,
                            separators=(",", ": "),
                            ensure_ascii=True,
                    )

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), "w") as outfile:
                outfile.write(out_str)
        else:
            raise VersionedDbExceptionFileMissing(
                    RepositoryConf.config_file_name()
            )

        return True

    @staticmethod
    def set_repo_env(repo_name: str, env: str, version: str):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES_PROP]
                    rp = [r for r in repos if r[NAME_PROP] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][ENVS_PROP] and env in rp[0][ENVS_PROP]:
                        rp[0][ENVS_PROP][env] = version
                    else:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(
                            repo_name, env
                        )

                    out_str = json.dumps(
                        conf,
                        indent=4,
                        sort_keys=True,
                        separators=(",", ": "),
                        ensure_ascii=True,
                    )

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

            with open(RepositoryConf.config_file_name(), "w") as outfile:
                outfile.write(out_str)
        else:
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        return True

    @staticmethod
    def get_repo_env(repo_name: str, env: str):
        conf = RepositoryConf._get_repo_dict()

        if os.path.isfile(RepositoryConf.config_file_name()):
            try:
                with open(RepositoryConf.config_file_name()):
                    repos = conf[REPOSITORIES_PROP]
                    rp = [r for r in repos if r[NAME_PROP] == repo_name]

                    if not rp:
                        raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                    if rp[0][ENVS_PROP] is None:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(
                            repo_name, env
                        )

                    if rp[0][ENVS_PROP] and env in rp[0][ENVS_PROP]:
                        return rp[0][ENVS_PROP][env]
                    else:
                        raise VersionedDbExceptionRepoEnvDoesNotExits(
                            repo_name, env
                        )

            except JSONDecodeError:
                raise VersionedDbExceptionBadConfigFile()

        else:
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

    @staticmethod
    def convert_dict_to_config_json_str(conf: Dict) -> str:
        return json.dumps(
            conf,
            indent=4,
            sort_keys=True,
            separators=(",", ": "),
            ensure_ascii=True,
        )

    @staticmethod
    def save_config(out_str: str):
        with open(RepositoryConf.config_file_name(), "w") as outfile:
            outfile.write(out_str)

    @staticmethod
    def get_repo_include_schemas(repo_name: str) -> Union[None, List[str]]:
        repo_dict = RepositoryConf.get_repo(repo_name)

        if INCLUDE_SCHEMAS_PROP in repo_dict:
            return repo_dict[INCLUDE_SCHEMAS_PROP]

        return None

    @staticmethod
    def get_repo_exclude_schemas(repo_name: str) -> Union[None, List[str]]:
        repo_dict = RepositoryConf.get_repo(repo_name)

        if EXCLUDE_SCHEMAS_PROP in repo_dict:
            return repo_dict[EXCLUDE_SCHEMAS_PROP]

        return None

    @staticmethod
    def get_repo_list(repo_name: str, list_name: str) -> Union[None, List[str]]:
        repo_dict = RepositoryConf.get_repo(repo_name)

        if list_name in repo_dict:
            return repo_dict[list_name]

        return None

    @staticmethod
    def check_include_exclude_violation(repo_name: str):
        inc_sch = RepositoryConf.get_repo_include_schemas(repo_name)
        exc_sch = RepositoryConf.get_repo_exclude_schemas(repo_name)

        if inc_sch and exc_sch and len(inc_sch) > 0 and len(exc_sch) > 0:
            raise VersionedDbExceptionIncludeExcludeSchema(repo_name)

    @staticmethod
    def balance_repo_lists(
        repo_name: str, add_list: List[str], add_to: str, remove_from: str
    ):
        conf = RepositoryConf._get_repo_dict()
        add_set = set(add_list)

        if not os.path.isfile(RepositoryConf.config_file_name()):
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        try:
            with open(RepositoryConf.config_file_name()):
                repos = conf[REPOSITORIES_PROP]
                rp = [r for r in repos if r[NAME_PROP] == repo_name]

                if not rp:
                    raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                if add_to in rp[0]:
                    current_set = set(rp[0][add_to])
                    add_set.union(current_set)

                rp[0][add_to] = list(add_set)

                if remove_from in rp[0]:
                    new_removes = set(rp[0][remove_from])
                    rp[0][remove_from] = list(new_removes.difference(add_set))

                out_str = RepositoryConf.convert_dict_to_config_json_str(conf)

        except JSONDecodeError:
            raise VersionedDbExceptionBadConfigFile()

        RepositoryConf.save_config(out_str)

        return True

    @staticmethod
    def remove_from_repo_list(
        repo_name: str, remove_list: List[str], remove_from: str
    ):
        conf = RepositoryConf._get_repo_dict()
        remove_set = set(remove_list)

        if not os.path.isfile(RepositoryConf.config_file_name()):
            raise VersionedDbExceptionFileMissing(
                RepositoryConf.config_file_name()
            )

        try:
            with open(RepositoryConf.config_file_name()):
                repos = conf[REPOSITORIES_PROP]
                rp = [r for r in repos if r[NAME_PROP] == repo_name]

                if not rp:
                    raise VersionedDbExceptionRepoDoesNotExits(repo_name)

                if remove_from in rp[0]:
                    current_set = set(rp[0][remove_from])
                    rp[0][remove_from] = list(
                        current_set.difference(remove_set)
                    )

                out_str = RepositoryConf.convert_dict_to_config_json_str(conf)

        except JSONDecodeError:
            raise VersionedDbExceptionBadConfigFile()

        RepositoryConf.save_config(out_str)

        return True
