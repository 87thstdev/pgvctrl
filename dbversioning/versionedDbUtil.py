import os
from collections import namedtuple
from typing import List

import simplejson as json
from os.path import join

import dbversioning.dbvctrlConst as Const
from dbversioning.versionedDbHelper import get_valid_elements
from dbversioning.errorUtil import (
    VersionedDbExceptionFileExits,
    VersionedDbExceptionVersionIsHigherThanApplying,
    VersionedDbExceptionRepoVersionExits,
    VersionedDbExceptionRepoVersionDoesNotExits,
    VersionedDbExceptionProductionChangeNoProductionFlag,
    VersionedDbExceptionMissingVersionTable,
    VersionedDbExceptionFastForwardNotAllowed,
    VersionedDbExceptionRepoDoesNotExits,
    VersionedDbExceptionRepoExits,
    VersionedDbExceptionEnvDoesMatchDbEnv,
    VersionedDbExceptionRepoVersionNumber,
    VersionedDbExceptionRepoNameInvalid)
from dbversioning.versionedDbShellUtil import (
    VersionDbShellUtil,
    error_message,
    information_message,
    repo_version_information_message,
    repo_unregistered_message,
    notice_message,
    warning_message,
    sql_rollback_information_message)
from dbversioning.versionedDb import (
    VersionDb,
    FastForwardDb,
    GenericSql)
from dbversioning.repositoryconf import (
    RepositoryConf,
    VERSION_STORAGE_PROP,
    SNAPSHOTS_DIR,
    FAST_FORWARD_DIR,
    DATABASE_BACKUP_DIR,
    ENVS_PROP,
    INCLUDE_SCHEMAS_PROP,
    EXCLUDE_SCHEMAS_PROP,
    INCLUDE_TABLES_PROP,
    EXCLUDE_TABLES_PROP,
    DUMP_DATABASE_OPTIONS_DEFAULT)

to_unicode = str

Version_Numbers = namedtuple(
    "version_numbers", ["major", "minor", "maintenance"]
)

DB_INIT_DISPLAY = "Database initialized"
DB_INIT_PRODUCTION_DISPLAY = DB_INIT_DISPLAY + " (PRODUCTION)"
DB_INIT_ENV = " environment ({0})"


class VersionedDbHelper:
    @staticmethod
    def __init__():
        pass

    @staticmethod
    def display_repo_list(verbose):
        """
        :return: list of database in config
        """
        db_repos = []
        conf = RepositoryConf()
        root = conf.root()

        ignored = {FAST_FORWARD_DIR, SNAPSHOTS_DIR, DATABASE_BACKUP_DIR}
        repo_locations = get_valid_elements(root, ignored)

        for repo_location in repo_locations:
            db_repos.append(VersionDb(join(os.getcwd(), root, repo_location)))

        for db_repo in db_repos:
            v_sorted = sorted(
                db_repo.versions,
                key=lambda vs: (vs.major, vs.minor, vs.maintenance),
            )
            repo_conf = conf.get_repo(db_repo.db_name)
            if repo_conf:
                information_message(db_repo.db_name)
                inc_exc = []
                if INCLUDE_SCHEMAS_PROP in repo_conf:
                    inc_exc.append(f"{INCLUDE_SCHEMAS_PROP}: {repo_conf[INCLUDE_SCHEMAS_PROP]}")

                if EXCLUDE_SCHEMAS_PROP in repo_conf:
                    inc_exc.append(f"{EXCLUDE_SCHEMAS_PROP}: {repo_conf[EXCLUDE_SCHEMAS_PROP]}")

                if INCLUDE_TABLES_PROP in repo_conf:
                    inc_exc.append(f"{INCLUDE_TABLES_PROP}: {repo_conf[INCLUDE_TABLES_PROP]}")

                if EXCLUDE_TABLES_PROP in repo_conf:
                    inc_exc.append(f"{EXCLUDE_TABLES_PROP}: {repo_conf[EXCLUDE_TABLES_PROP]}")

                if inc_exc:
                    msg = ", ".join(inc_exc)
                    notice_message(f"\t({msg})")
            else:
                repo_unregistered_message(db_repo.db_name)

            for v in v_sorted:
                env = []
                if repo_conf and repo_conf[ENVS_PROP]:
                    for e in repo_conf[ENVS_PROP]:
                        if repo_conf[ENVS_PROP][e] == v.version_number:
                            env.append(e)
                if env:
                    repo_version_information_message(f"\tv {v.full_name}", f"{env}")
                else:
                    repo_version_information_message(f"\tv {v.full_name}", "")

                if verbose:
                    for s in v.sql_files:
                        sql_msg = f"\t\t{s.number} {s.name}"
                        if s.is_rollback:
                            sql_rollback_information_message(sql_message=sql_msg)
                        else:
                            information_message(message=sql_msg)

    @staticmethod
    def valid_repository(repository: str):
        found = True
        conf = RepositoryConf()
        root = conf.root()
        try:
            VersionDb(join(os.getcwd(), root, repository))
        except (OSError, TypeError, VersionedDbExceptionRepoDoesNotExits):
            found = False
        except VersionedDbExceptionRepoNameInvalid:
            raise VersionedDbExceptionRepoNameInvalid(repo_name=repository)

        return found

    @staticmethod
    def valid_repository_throwing(repository: str):
        repo_found = VersionedDbHelper.valid_repository(repository)
        if not repo_found:
            raise VersionedDbExceptionRepoDoesNotExits(repository)

    @staticmethod
    def get_repository_version(repository, version):
        conf = RepositoryConf()
        root = conf.root()

        vdb = VersionDb(join(os.getcwd(), root, repository))

        rtn = [
            v
            for v in vdb.versions
            if v.major == version.major
            and v.minor == version.minor
            and v.maintenance == version.maintenance
        ]

        if len(rtn) == 0:
            return None

        return rtn

    @staticmethod
    def create_repository_version(repository, version):
        conf = RepositoryConf()
        root = conf.root()
        vdb = VersionDb(join(os.getcwd(), root, repository))
        if vdb.create_version(version):
            information_message(f"Version {repository}/{version} created.")

    @staticmethod
    def get_repository_fast_forward_version(repository, version):
        conf = RepositoryConf()

        vdb = FastForwardDb(
            join(os.getcwd(), conf.fast_forward_dir(), repository)
        )

        rtn = [v for v in vdb.fast_forward_versions if v.full_name == version]

        if len(rtn) == 0:
            return None

        return rtn

    @staticmethod
    def display_db_version_on_server(db_conn, repo_name):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        VersionDbShellUtil.display_db_instance_version(v_stg, db_conn)

    @staticmethod
    def initialize_db_version_on_server(
        db_conn, repo_name, is_production, env=None
    ):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        if VersionDbShellUtil.init_db(
            repo_name=repo_name,
            v_stg=v_stg,
            db_conn=db_conn,
            is_production=is_production,
            env=env,
        ):

            if is_production:
                msg = DB_INIT_PRODUCTION_DISPLAY
            else:
                msg = DB_INIT_DISPLAY

            if env:
                msg += DB_INIT_ENV.format(env)

            information_message(msg)

    @staticmethod
    def apply_repository_fast_forward_to_database(
        repo_name, db_conn, full_version
    ):
        has_version_tbl = True
        try:
            v_stg = VersionedDbHelper._get_v_stg(repo_name)
            VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)
        except VersionedDbExceptionMissingVersionTable:
            has_version_tbl = False

        if has_version_tbl:
            raise VersionedDbExceptionFastForwardNotAllowed()

        fast_forward_to = VersionedDbHelper.get_repository_fast_forward_version(
            repo_name, full_version
        )
        if fast_forward_to:
            VersionDbShellUtil.apply_fast_forward_sql(
                db_conn, fast_forward_to[0], repo_name
            )
        else:
            error_message(f"Fast forward not found {full_version}")

    @staticmethod
    def push_data_to_database(repo_name, db_conn, force, is_production, table_list=None):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        if is_production != dbver.is_production:
            raise VersionedDbExceptionProductionChangeNoProductionFlag(
                Const.PUSH_DATA_ARG
            )

        conf = RepositoryConf()
        data_files = []
        data_dump = conf.get_data_dump_dir(repo_name)
        data_push_set = VersionDbShellUtil.get_data_dump_dict(repo_name)
        if table_list:
            data_push_set = [t for t in data_push_set if t[Const.DATA_TABLE] in table_list]
        apply_order = set([p[Const.DATA_APPLY_ORDER] for p in data_push_set])

        if len(data_push_set) == 0:
            warning_message("No tables found to push")

        pre_sql = VersionedDbHelper._get_wrapper_push_sql(data_dump, Const.DATA_PRE_PUSH_FILE)
        if pre_sql:
            data_files.append(pre_sql)

        for o in apply_order:
            for data_table in data_push_set:
                if data_table[Const.DATA_APPLY_ORDER] == o:
                    sql_path = os.path.join(data_dump, f"{data_table[Const.DATA_TABLE]}.sql")
                    gs = GenericSql(sql_path)
                    data_files.append(gs)

        post_sql = VersionedDbHelper._get_wrapper_push_sql(data_dump, Const.DATA_POST_PUSH_FILE)
        if post_sql:
            data_files.append(post_sql)

        VersionedDbHelper.apply_data_sql_files_to_database(
            db_conn, data_files, force
        )

    @staticmethod
    def _get_wrapper_push_sql(data_path: str, file_name: str):
        post_name = os.path.join(data_path, file_name)
        if os.path.isfile(post_name):
            return GenericSql(post_name)

        return None

    @staticmethod
    def repo_database_dump(repo_name, db_conn, is_production):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        if is_production != dbver.is_production:
            raise VersionedDbExceptionProductionChangeNoProductionFlag(
                Const.DUMP_DATABASE_ARG
            )
        # dump_database_backup
        conf = RepositoryConf()
        dump_options = conf.get_repo_dump_database_options(repo_name=repo_name)

        if not dump_options:
            dump_options = DUMP_DATABASE_OPTIONS_DEFAULT

        dump_options_list = dump_options.split(" ")

        VersionDbShellUtil.dump_database_backup(db_conn, v_stg, dump_options_list)
        information_message(f"Repository {repo_name} database backup")


    @staticmethod
    def pull_table_for_repo_data(repo_name, db_conn, table_list=None):
        if table_list:
            VersionDbShellUtil.pull_tables_from_database(
                repo_name, db_conn, table_list
            )
        else:
            VersionDbShellUtil.pull_repo_tables_from_database(
                repo_name, db_conn
            )

    @staticmethod
    def apply_repository_to_database(
        repo_name, db_conn, version, is_production=False, env=None
    ):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)
        repo_nums = None

        if is_production != dbver.is_production:
            raise VersionedDbExceptionProductionChangeNoProductionFlag(
                Const.APPLY_ARG
            )

        if f"{env}" != f"{dbver.env}":
            raise VersionedDbExceptionEnvDoesMatchDbEnv(
                env=env, db_env=dbver.env
            )

        if dbver.version:
            repo_nums = VersionedDbHelper.get_version_numbers(dbver.version)

        ver_nums = VersionedDbHelper.get_version_numbers(version)

        repo_ver = VersionedDbHelper.get_repository_version(repo_name, ver_nums)

        if repo_ver is None:
            raise VersionedDbExceptionRepoVersionDoesNotExits(
                repo_name, version
            )

        apply_repo = repo_ver[0]

        standing = VersionedDbHelper._version_standing(ver_nums, repo_nums)
        if standing < 0:
            raise VersionedDbExceptionVersionIsHigherThanApplying(
                dbver.version, version
            )

        VersionedDbHelper.apply_sql_files_to_database(
            db_conn, apply_repo.sql_files
        )

        # Add snapshot
        if RepositoryConf.auto_snapshots():
            VersionDbShellUtil.dump_version_snapshot(db_conn, v_stg)

        ver_hash = apply_repo.get_version_hash_set()

        increase_rev = True
        reset_rev = False
        if standing > 0:
            increase_rev = False
            reset_rev = True

        VersionDbShellUtil.set_db_instance_version(
            db_conn=db_conn,
            v_stg=v_stg,
            new_version=apply_repo.full_name,
            new_hash=ver_hash,
            increase_rev=increase_rev,
            reset_rev=reset_rev,
        )
        new_dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        information_message(
            f"Applied: {new_dbver.repo_name} v {new_dbver.version}.{new_dbver.revision}"
        )
        return True

    @staticmethod
    def set_repository_fast_forward(repo_name, db_conn):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)

        if VersionDbShellUtil.dump_version_fast_forward(
            db_conn, v_stg, repo_name
        ):
            information_message(f"Fast forward set: {repo_name}")

    @staticmethod
    def apply_sql_files_to_database(db_conn, sql_files):
        for sql_file in sql_files:
            if not sql_file.is_rollback:
                VersionDbShellUtil.apply_sql_file(db_conn, sql_file)

    @staticmethod
    def apply_data_sql_files_to_database(db_conn, sql_files, force=None):
        for sql_file in sql_files:
            VersionDbShellUtil.apply_data_sql_file(db_conn, sql_file, force)

    @staticmethod
    def _version_standing(v_h, v_l):
        if v_l is None or v_h.major > v_l.major:
            return 1

        if v_h.major == v_l.major and v_h.minor > v_l.minor:
            return 1

        if (
            v_h.major == v_l.major
            and v_h.minor == v_l.minor
            and v_h.maintenance > v_l.maintenance
        ):
            return 1

        if (
            v_h.major == v_l.major
            and v_h.minor == v_l.minor
            and v_h.maintenance == v_l.maintenance
        ):
            return 0

        return -1

    @staticmethod
    def create_repository_version_folder(repo_name, version):
        version_nums = VersionedDbHelper.get_version_numbers(version)
        version_found = VersionedDbHelper.get_repository_version(
            repo_name, version_nums
        )

        if version_found:
            raise VersionedDbExceptionRepoVersionExits(
                repo_name, version_found[0]
            )

        VersionedDbHelper.create_repository_version(repo_name, version)

    @staticmethod
    def create_repository(repo_name):
        repo_found = VersionedDbHelper.valid_repository(repo_name)
        if repo_found:
            raise VersionedDbExceptionRepoExits(repo_name)

        if RepositoryConf.create_repo(repo_name=repo_name):
            information_message(f"Repository created: {repo_name}")

    @staticmethod
    def remove_repository(repo_name):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.remove_repo(repo_name=repo_name):
            information_message(f"Repository removed: {repo_name}")

    @staticmethod
    def create_repository_environment(repo_name, env):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.create_repo_env(repo_name=repo_name, env=env):
            information_message(
                f"Repository environment created: {repo_name} {env}"
            )

    @staticmethod
    def remove_repository_environment(repo_name, env):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.remove_repo_env(repo_name=repo_name, env=env):
            information_message(
                f"Repository environment removed: {repo_name} {env}"
            )

    @staticmethod
    def get_repository_environment(repo_name, env):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        return RepositoryConf.get_repo_env(repo_name=repo_name, env=env)

    @staticmethod
    def set_repository_version_storage_owner(repo_name: str, owner: str):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.set_repo_version_storage_owner(repo_name=repo_name, owner=owner):
            information_message(
                f"Repository version storage owner set: {repo_name} {owner}"
            )

    @staticmethod
    def set_repository_environment_version(repo_name, env, version):
        VersionedDbHelper.valid_repository_throwing(repo_name)
        version_nums = VersionedDbHelper.get_version_numbers(version)
        version_found = VersionedDbHelper.get_repository_version(
            repo_name, version_nums
        )

        if not version_found:
            raise VersionedDbExceptionRepoVersionDoesNotExits(
                repo_name, version
            )

        if RepositoryConf.set_repo_env(
            repo_name=repo_name, env=env, version=version_found[0].version_number
        ):
            information_message(
                f"Repository environment set: {repo_name} {env} {version}"
            )

    @staticmethod
    def add_repository_include_schemas(repo_name, include_schemas):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.balance_repo_lists(
            repo_name=repo_name,
            add_list=include_schemas,
            add_to=INCLUDE_SCHEMAS_PROP,
            remove_from=EXCLUDE_SCHEMAS_PROP,
        ):
            information_message(
                f"Repository added: {repo_name} include-schemas {include_schemas}"
            )

    @staticmethod
    def remove_repository_include_schemas(repo_name: str, rminclude_schemas: List[str]):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.remove_from_repo_list(
            repo_name=repo_name,
            remove_list=rminclude_schemas,
            remove_from=INCLUDE_SCHEMAS_PROP,
        ):
            information_message(
                f"Repository removed: {repo_name} include-schemas {rminclude_schemas}"
            )

    @staticmethod
    def add_repository_exclude_schemas(repo_name, exclude_schemas):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.balance_repo_lists(
            repo_name=repo_name,
            add_list=exclude_schemas,
            add_to=EXCLUDE_SCHEMAS_PROP,
            remove_from=INCLUDE_SCHEMAS_PROP,
        ):
            information_message(
                f"Repository added: {repo_name} exclude-schemas {exclude_schemas}"
            )

    @staticmethod
    def remove_repository_exclude_schemas(repo_name, rmexclude_schemas):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.remove_from_repo_list(
            repo_name=repo_name,
            remove_list=rmexclude_schemas,
            remove_from=EXCLUDE_SCHEMAS_PROP,
        ):
            information_message(
                f"Repository removed: {repo_name} exclude-schemas {rmexclude_schemas}"
            )

    @staticmethod
    def add_repository_include_table(repo_name, include_tables):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.balance_repo_lists(
            repo_name=repo_name,
            add_list=include_tables,
            add_to=INCLUDE_TABLES_PROP,
            remove_from=EXCLUDE_TABLES_PROP,
        ):
            information_message(
                f"Repository added: {repo_name} include-table {include_tables}"
            )

    @staticmethod
    def remove_repository_include_table(repo_name: str, rminclude_table: List[str]):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.remove_from_repo_list(
            repo_name=repo_name,
            remove_list=rminclude_table,
            remove_from=INCLUDE_TABLES_PROP,
        ):
            information_message(
                f"Repository removed: {repo_name} include-table {rminclude_table}"
            )

    @staticmethod
    def add_repository_exclude_table(repo_name, exclude_tables):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.balance_repo_lists(
            repo_name=repo_name,
            add_list=exclude_tables,
            add_to=EXCLUDE_TABLES_PROP,
            remove_from=INCLUDE_TABLES_PROP,
        ):
            information_message(
                f"Repository added: {repo_name} exclude-table {exclude_tables}"
            )

    @staticmethod
    def remove_repository_exclude_table(repo_name, rmexclude_table):
        VersionedDbHelper.valid_repository_throwing(repo_name)

        if RepositoryConf.remove_from_repo_list(
            repo_name=repo_name,
            remove_list=rmexclude_table,
            remove_from=EXCLUDE_TABLES_PROP,
        ):
            information_message(
                f"Repository removed: {repo_name} exclude-table {rmexclude_table}"
            )

    @staticmethod
    def create_config():
        conf = RepositoryConf()
        if not os.path.isfile(conf.config_file_name()):
            with open(conf.config_file_name(), "w") as outfile:
                str_ = json.dumps(
                    conf.config_json(),
                    indent=4,
                    sort_keys=True,
                    separators=(",", ": "),
                    ensure_ascii=True,
                )
                outfile.write(to_unicode(str_))
            information_message(
                f"Config file created: {conf.config_file_name()}"
            )
        else:
            raise VersionedDbExceptionFileExits(conf.config_file_name())

    @staticmethod
    def _get_v_stg(repo_name):
        repo = RepositoryConf.get_repo(repo_name)
        v_stg = None

        if repo:
            v_stg = repo[VERSION_STORAGE_PROP]
        else:
            raise VersionedDbExceptionRepoDoesNotExits(repo_name)

        return v_stg

    @staticmethod
    def get_version_numbers(version: str):
        try:
            ver_array = version.split(".")

            return Version_Numbers(
                int(ver_array[0]), int(ver_array[1]), int(ver_array[2])
            )
        except (ValueError, AttributeError):
            raise VersionedDbExceptionRepoVersionNumber(version=version)
