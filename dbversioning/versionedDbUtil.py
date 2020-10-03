import os
from collections import namedtuple
from typing import List, Optional

import json
from os.path import join

import dbversioning.dbvctrlConst as Const
from dbversioning.osUtil import dir_exists
from dbversioning.versionedDbHelper import get_valid_elements
from dbversioning.errorUtil import (
    VersionedDbExceptionFileExits,
    VersionedDbExceptionVersionIsHigherThanApplying,
    VersionedDbExceptionRepoVersionExits,
    VersionedDbExceptionRepoVersionDoesNotExits,
    VersionedDbExceptionProductionChangeNoProductionFlag,
    VersionedDbExceptionSchemaSnapshotNotAllowed,
    VersionedDbExceptionRepoDoesNotExits,
    VersionedDbExceptionRepoExits,
    VersionedDbExceptionEnvDoesMatchDbEnv,
    VersionedDbExceptionRepoVersionNumber,
    VersionedDbExceptionRepoNameInvalid,
    VersionedDbExceptionRestoreNotAllowed,
    VersionedDbExceptionFileMissing,
    VersionedDbExceptionNoVersionFound,
    VersionedDbExceptionMissingRepo)
from dbversioning.versionedDbShellUtil import (
    VersionDbShellUtil,
    error_message,
    get_file_size,
    get_time_text,
    information_message,
    repo_version_information_message,
    repo_unregistered_message,
    notice_message,
    warning_message,
    sql_rollback_information_message,
    sql_applied_message,
    sql_not_applied_message,
    sql_different_message,
    sql_missing_applied_message)
from dbversioning.versionedDb import (
    VersionDb,
    SchemaSnapshot,
    GenericSql,
    SchemaSnapshotVersion,
    get_file_hash)
from dbversioning.repositoryconf import (
    RepositoryConf,
    VERSION_STORAGE_PROP,
    SCHEMA_SNAPSHOT_DIR,
    DATABASE_BACKUP_DIR,
    ENVS_PROP,
    INCLUDE_SCHEMAS_PROP,
    EXCLUDE_SCHEMAS_PROP,
    INCLUDE_TABLES_PROP,
    EXCLUDE_TABLES_PROP,
    DUMP_OPTIONS_DEFAULT,
    RESTORE_OPTIONS_DEFAULT)

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
    def display_repo_list(verbose) -> bool:
        """
        :return: list of database in config
        """
        db_repos = []
        conf = RepositoryConf()
        root = conf.root()

        ignored = {SCHEMA_SNAPSHOT_DIR, DATABASE_BACKUP_DIR}
        repo_locations = get_valid_elements(root, ignored)
        if len(repo_locations) == 0:
            return False

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
                    notice_message(f"{Const.TAB}({msg})")
            else:
                repo_unregistered_message(db_repo.db_name)

            for v in v_sorted:
                env = []
                if repo_conf and repo_conf[ENVS_PROP]:
                    for e in repo_conf[ENVS_PROP]:
                        if repo_conf[ENVS_PROP][e] == v.version_number:
                            env.append(e)
                if env:
                    repo_version_information_message(f"{Const.TAB}v {v.full_name}", f"{env}")
                else:
                    repo_version_information_message(f"{Const.TAB}v {v.full_name}", "")

                if verbose:
                    for s in v.sql_files:
                        sql_msg = f"{Const.TABS}{s.number} {s.name}"
                        if s.is_rollback:
                            sql_rollback_information_message(sql_message=sql_msg)
                        else:
                            information_message(message=sql_msg)

        return True

    @staticmethod
    def display_db_version_status(v_tbl, repo_name, db_conn):
        dbv = VersionDbShellUtil.get_db_instance_version(v_tbl, db_conn)
        if dbv and dbv.version is None:
            raise VersionedDbExceptionNoVersionFound()

        conf = RepositoryConf()
        root = conf.root()

        ignored = {SCHEMA_SNAPSHOT_DIR, DATABASE_BACKUP_DIR}
        repo_locations = get_valid_elements(root, ignored)

        if repo_name not in repo_locations:
            raise VersionedDbExceptionMissingRepo(repo_name)

        db_repo = VersionDb(join(os.getcwd(), root, repo_name))
        v = [ver for ver in db_repo.versions if ver.full_name == dbv.version]

        if not v:
            raise VersionedDbExceptionRepoVersionDoesNotExits(
                    repo_name=repo_name,
                    version_name=dbv.version
            )

        version = v[0]

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
                notice_message(f"{Const.TAB}({msg})")
        else:
            repo_unregistered_message(db_repo.db_name)

        env = []
        if repo_conf and repo_conf[ENVS_PROP]:
            for e in repo_conf[ENVS_PROP]:
                if repo_conf[ENVS_PROP][e] == version.version_number:
                    env.append(e)
        if env:
            repo_version_information_message(f"{Const.TAB}v {version.full_name}", f"{env}")
        else:
            repo_version_information_message(f"{Const.TAB}v {version.full_name}", "")

        file_hashes = json.loads(dbv.version_hash)
        for fh in file_hashes:
            name_array = fh["file"].split(".")
            fh["number"] = int(name_array[0])
            fh["name"] = ".".join(name_array[1:(len(name_array) - 1)])

        for s in version.sql_files:
            if s.is_rollback:
                continue

            fh = [f for f in file_hashes if f["file"] == s.fullname]
            if fh and fh[0]:
                sql_hash = get_file_hash(file_path=s.path)
                fh[0]["hash_match"] = sql_hash == fh[0]["hash"]
                fh[0]["db_has_sql"] = True
            else:
                file_hashes.append({
                    "number": s.number,
                    "name": s.name,
                    "file": s.fullname,
                    "hash_match": False,
                    "db_has_sql": False,
                })
        sorted_h = sorted(
                file_hashes,
                key=lambda file: (file["number"], file["name"]),
        )
        for h in sorted_h:
            if "hash_match" not in h:
                sql_missing_applied_message(sql_name=h["file"])
                continue

            if h["db_has_sql"] and h["hash_match"]:
                sql_applied_message(sql_name=h["file"])
                continue

            if h["db_has_sql"] is False:
                sql_not_applied_message(sql_name=h["file"])
                continue

            if h["db_has_sql"] is True and h["hash_match"] is False:
                sql_different_message(sql_name=h["file"])

    @staticmethod
    def display_repo_ss_list() -> bool:
        """
        :return: list of repository version Schema Snapshots
        """
        has_sss = False
        conf = RepositoryConf()
        root = conf.root()
        ss_root = f"{root}/{SCHEMA_SNAPSHOT_DIR}"

        if not dir_exists(ss_root):
            return False

        ignored = {root, SCHEMA_SNAPSHOT_DIR, DATABASE_BACKUP_DIR}
        repo_ss_locations = get_valid_elements(ss_root, ignored)
        ss_sql_ver = []
        for repo_location in repo_ss_locations:
            has_sss = True
            ss_locations = get_valid_elements(f"{ss_root}/{repo_location}")
            information_message(repo_location)
            for ss_sql in ss_locations:
                ss_sql_ver.append(SchemaSnapshotVersion(ss_sql))

            def _none_to_0(val):
                return 0 if val is None else val

            ss_sql_ver = sorted(
                    ss_sql_ver,
                    key=lambda vs: (_none_to_0(vs.major), _none_to_0(vs.minor), _none_to_0(vs.maintenance), vs.full_name),
            )

            for ss_v in ss_sql_ver:
                sql_file_path = f"{ss_root}/{repo_location}/{ss_v.sql_file}"
                information_message(f"{Const.TAB}{ss_v.full_name}{Const.TAB}{get_file_size(sql_file_path)}")

            ss_sql_ver = []

        return has_sss

    @staticmethod
    def display_repo_dd_list():
        """
        :return: list of repository version database dumps
        """
        has_dd = False
        conf = RepositoryConf()
        root = conf.root()
        dd_root = f"{root}/{DATABASE_BACKUP_DIR}"

        ignored = {root, SCHEMA_SNAPSHOT_DIR}

        if not dir_exists(dd_root):
            return False

        repo_dd_locations = get_valid_elements(dd_root, ignored)

        for repo_location in repo_dd_locations:
            has_dd = True
            dd_locations = get_valid_elements(f"{dd_root}/{repo_location}")
            information_message(repo_location)

            dd_locations = sorted(dd_locations)

            for dd_file in dd_locations:
                dd_file_path = f"{dd_root}/{repo_location}/{dd_file}"
                information_message(f"{Const.TAB}{dd_file:<30}{get_file_size(dd_file_path):>15}")

        return has_dd

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
        # TODO: Fix, return none or VersionDb
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
    def remove_repository_version(repository: str, version: str, version_nums: str):
        conf = RepositoryConf()
        root = conf.root()
        vdb = VersionDb(join(os.getcwd(), root, repository))
        if vdb.remove_version(version):
            conf.remove_repo_version(repo_name=repository, version_nums=version_nums)
            information_message(f"Version {repository}/{version} removed.")
        else:
            error_message(f"Version {repository}/{version} does not exits.")

    @staticmethod
    def get_repository_schema_snapshot_version(repository, version):
        conf = RepositoryConf()

        vdb = SchemaSnapshot(
            join(os.getcwd(), conf.schema_snapshot_dir(), repository)
        )

        rtn = [v for v in vdb.schema_snapshot_versions if v.full_name == version]

        if len(rtn) == 0:
            return None

        return rtn

    @staticmethod
    def display_db_version_on_server(db_conn, repo_name):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        VersionDbShellUtil.display_db_instance_version(v_stg, db_conn)

    @staticmethod
    def display_db_version_status_on_server(db_conn, repo_name):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        VersionedDbHelper.display_db_version_status(v_stg, repo_name, db_conn)

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
    def apply_repository_schema_snapshot_to_database(
        repo_name, db_conn, full_version, force: bool = False
    ):
        if not VersionDbShellUtil.is_database_empty(db_conn):
            raise VersionedDbExceptionSchemaSnapshotNotAllowed()

        schema_snapshot_to = VersionedDbHelper.get_repository_schema_snapshot_version(
            repo_name, full_version
        )
        if schema_snapshot_to:
            VersionDbShellUtil.apply_schema_snapshot_sql(
                db_conn, schema_snapshot_to[0]
            )
        else:
            error_message(f"Schema snapshot not found {full_version}")

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

        total_time = VersionedDbHelper.apply_data_sql_files_to_database(
            db_conn, data_files, force
        )

        if RepositoryConf.is_timer_on():
            notice_message(f"Total time: {get_time_text(total_time)}\n")

    @staticmethod
    def _get_wrapper_push_sql(data_path: str, file_name: str):
        post_name = os.path.join(data_path, file_name)
        if os.path.isfile(post_name):
            return GenericSql(post_name)

        return None

    @staticmethod
    def repo_database_dump(repo_name, db_conn, name: str, is_production: bool):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        if is_production != dbver.is_production:
            raise VersionedDbExceptionProductionChangeNoProductionFlag(
                Const.DUMP_ARG
            )

        conf = RepositoryConf()
        dump_options = conf.get_repo_dump_options(repo_name=repo_name)

        if not dump_options:
            dump_options = DUMP_OPTIONS_DEFAULT

        dump_options_list = dump_options.split(" ")
        exec_time = VersionDbShellUtil.dump_backup(
                db_conn=db_conn,
                v_stg=v_stg,
                dump_options=dump_options_list,
                name=name
        )

        msg = f"Repository {repo_name} database backed up"
        if RepositoryConf.is_timer_on():
            information_message(f"{msg} (time: {get_time_text(exec_time)})\n")
        else:
            information_message(msg)

    @staticmethod
    def repo_database_restore(db_conn, repo_name: str, file_name: str):
        VersionedDbHelper.valid_repository(repo_name)
        conf = RepositoryConf()

        repo_db_bak = os.path.join(conf.database_backup_dir(), repo_name)
        file_path = os.path.join(repo_db_bak, file_name)

        if not os.path.isfile(file_path):
            raise VersionedDbExceptionFileMissing(file_path)

        if not VersionDbShellUtil.is_database_empty(db_conn):
            raise VersionedDbExceptionRestoreNotAllowed()

        conf = RepositoryConf()
        restore_options = conf.get_repo_restore_options(repo_name=repo_name)

        if not restore_options:
            restore_options = RESTORE_OPTIONS_DEFAULT

        restore_options_list = restore_options.split(" ")

        exec_time = VersionDbShellUtil.restore_backup(
                db_conn=db_conn,
                restore_options=restore_options_list,
                file_path=file_path
        )

        msg = f"Database {file_name} from repository {repo_name} restored {db_conn}."
        if RepositoryConf.is_timer_on():
            information_message(f"{msg} (time: {get_time_text(exec_time)})\n")
        else:
            information_message(msg)

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

        total_time = VersionedDbHelper.apply_sql_files_to_database(
            db_conn, apply_repo.sql_files
        )

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
        if RepositoryConf.is_timer_on():
            notice_message(f"{Const.TAB}Total time: {get_time_text(total_time)}\n")
        return True

    @staticmethod
    def set_repository_schema_snapshot(repo_name, db_conn, name: str):
        v_stg = VersionedDbHelper._get_v_stg(repo_name)
        file_name, exec_time = VersionDbShellUtil.dump_version_schema_snapshot(
            db_conn=db_conn, v_stg=v_stg, repo_name=repo_name, name=name
        )

        if file_name:
            msg = f"Schema snapshot set: {repo_name} ({file_name})"
            if RepositoryConf.is_timer_on():
                information_message(f"{msg} (time: {get_time_text(exec_time)})\n")
            else:
                information_message(msg)

    @staticmethod
    def apply_sql_files_to_database(db_conn, sql_files) -> Optional[float]:
        total_time = 0.00

        for sql_file in sql_files:
            if not sql_file.is_rollback:
                rtn = VersionDbShellUtil.apply_sql_file(db_conn, sql_file)
                if rtn:
                    total_time += rtn

        return total_time

    @staticmethod
    def apply_data_sql_files_to_database(db_conn, sql_files, force=None) -> Optional[float]:
        total_time = 0.00
        for sql_file in sql_files:
            rtn = VersionDbShellUtil.apply_data_sql_file(db_conn, sql_file, force)
            if rtn:
                total_time += rtn

        return total_time

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
    def remove_repository_version_folder(repo_name, version):
        version_nums = VersionedDbHelper.get_version_numbers(version)
        version_found = VersionedDbHelper.get_repository_version(
                repo_name, version_nums
        )

        if version_found is None:
            raise VersionedDbExceptionRepoVersionDoesNotExits(
                    repo_name=repo_name, version_name=version
            )

        VersionedDbHelper.remove_repository_version(repo_name, version, version_found[0].version_number)

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
    def set_timer(state: bool):
        state_str = "ON" if state else "OFF"

        RepositoryConf.set_timer(state=state)
        information_message(f"Execution Timer {state_str}")

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
