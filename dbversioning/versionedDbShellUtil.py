import os

import datetime
import sys
from typing import List, Union

import simplejson as json

import copy
from plumbum import colors, local, ProcessExecutionError
from simplejson import JSONDecodeError

import dbversioning.dbvctrlConst as Const
from dbversioning.osUtil import ensure_dir_exists, make_data_file
from dbversioning.versionedDb import SqlPatch
from dbversioning.errorUtil import (
    VersionedDbExceptionMissingVersionTable,
    VersionedDbExceptionBadDateSource,
    VersionedDbExceptionNoVersionFound,
    VersionedDbException,
    VersionedDbExceptionTooManyVersionRecordsFound,
    VersionedDbExceptionDatabaseAlreadyInit,
    VersionedDbExceptionSqlExecutionError,
    VersionedDbExceptionBadDataConfigFile
)
from dbversioning.repositoryconf import (
    RepositoryConf,
    ROLLBACK_FILE_ENDING,
    INCLUDE_TABLES_PROP,
    EXCLUDE_TABLES_PROP,
    Version_Table)

DATA_DUMP_CONFIG_NAME = "data.json"
RETCODE = 0
STDOUT = 1
STDERR = 2

SNAPSHOT_DATE_FORMAT = "%Y%m%d%H%M%S"

to_unicode = str


class DatabaseRepositoryVersion(object):
    def __init__(
        self,
        version=None,
        revision=None,
        repo_name=None,
        is_production=None,
        version_hash=None,
        env=None,
    ):
        self.version = version
        self.revision = revision
        self.repo_name = repo_name
        self.is_production = is_production
        self.version_hash = version_hash
        self.env = env


class VersionDbShellUtil:
    @staticmethod
    def __init__():
        pass

    @staticmethod
    def init_db(
        repo_name: str, v_stg: Version_Table=None, db_conn=None, is_production: bool=False, env: str=None
    ):
        psql = _local_psql()
        conf = RepositoryConf()
        missing_tbl = False

        try:
            dbv = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)
            if dbv:
                raise VersionedDbExceptionDatabaseAlreadyInit()

        except VersionedDbExceptionMissingVersionTable:
            missing_tbl = True

        create_v_tbl = f"CREATE TABLE IF NOT EXISTS {v_stg.tbl} (" \
                       f"{v_stg.v} VARCHAR," \
                       f"{v_stg.repo} VARCHAR NOT NULL," \
                       f"{v_stg.is_prod} BOOLEAN NOT NULL," \
                       f"{v_stg.env} VARCHAR," \
                       f"{v_stg.rev} INTEGER NOT NULL DEFAULT(0)," \
                       f"{v_stg.hash} JSONB);"

        if v_stg.owner:
            create_v_tbl = f"SET ROLE {v_stg.owner}; {create_v_tbl} RESET ROLE;"

        if env:
            env_var = f"'{env}'"
        else:
            env_var = "NULL"

        insert_v_sql = f"INSERT INTO {v_stg.tbl} ({v_stg.repo}, {v_stg.is_prod}, {v_stg.env}) "
        insert_v_sql += f"VALUES ('{repo_name}', '{is_production}', {env_var});"

        if missing_tbl:
            psql(db_conn, "-c", create_v_tbl, retcode=0)

        psql(db_conn, "-c", insert_v_sql, retcode=0)

        ensure_dir_exists(os.path.join(conf.root(), repo_name))

        return True

    @staticmethod
    def apply_fast_forward_sql(db_conn, sql_file, repo_name):
        psql = _local_psql()
        psql_parm_list = copy.copy(db_conn)

        psql_parm_list.append("-f")
        psql_parm_list.append(sql_file.sql_file)
        psql_parm_list.append(Const.V_ARG)
        psql_parm_list.append("ON_ERROR_STOP=1")
        try:
            information_message(f"Applying: {sql_file.full_name}")
            rtn = psql.run(psql_parm_list, retcode=0)
            msg = rtn[2]
            msg_formatted = msg.replace(f"psql:{sql_file.full_name}:", "\t")
            information_message(f"{msg_formatted}")

            # TODO: Make this happen, could be an -init call?
            # v_stg = VersionedDbHelper._get_v_stg(repo_name)
            # DbVersionShellHelper.set_db_instance_version(db_conn, v_stg, sql_file.full_name)

        except ProcessExecutionError as e:
            raise VersionedDbExceptionSqlExecutionError(e.stderr)

    @staticmethod
    def pull_tables_from_database(repo_name, db_conn, table_list):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()

        ensure_dir_exists(conf.get_data_dump_dir(repo_name))

        for tbl in table_list:
            sql_loc = conf.get_data_dump_sql_dir(repo_name, f"{tbl}.sql")

            pg_dump_parm_list = copy.copy(db_conn)
            if VersionDbShellUtil.get_col_inserts_setting(repo_name, tbl):
                pg_dump_parm_list.append("--column-inserts")
            pg_dump_parm_list.append("--disable-triggers")
            pg_dump_parm_list.append("--if-exists")
            pg_dump_parm_list.append("-c")
            pg_dump_parm_list.append(Const.TBL_ARG)
            pg_dump_parm_list.append(tbl)
            pg_dump_parm_list.append("-f")
            pg_dump_parm_list.append(sql_loc)

            try:
                information_message(f"Pulling: {tbl}")
                pg_dump(pg_dump_parm_list, retcode=0)
            except ProcessExecutionError as e:
                raise VersionedDbExceptionSqlExecutionError(e.stderr)

    @staticmethod
    def pull_repo_tables_from_database(repo_name, db_conn):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()

        ensure_dir_exists(conf.get_data_dump_dir(repo_name))
        table_list = VersionDbShellUtil.get_data_dump_dict(repo_name)

        if len(table_list) == 0:
            raise VersionedDbException("No tables to pull!")

        for tbl in table_list:
            sql_loc = conf.get_data_dump_sql_dir(
                repo_name, f"{tbl['table']}.sql"
            )

            pg_dump_parm_list = copy.copy(db_conn)
            if tbl["column-inserts"]:
                pg_dump_parm_list.append("--column-inserts")
            pg_dump_parm_list.append("--disable-triggers")
            pg_dump_parm_list.append("--if-exists")
            pg_dump_parm_list.append("-c")
            pg_dump_parm_list.append(Const.TBL_ARG)
            pg_dump_parm_list.append(tbl["table"])
            pg_dump_parm_list.append("-f")
            pg_dump_parm_list.append(sql_loc)

            size_num, size_txt = get_table_size(tbl, db_conn)
            information_message(f"Pulling: {tbl['table']}, {size_txt}")
            pg_dump(pg_dump_parm_list, retcode=0)

    @staticmethod
    def apply_sql_file(db_conn, sql_file, break_on_error=False):
        psql = _local_psql()
        psql_parm_list = copy.copy(db_conn)

        psql_parm_list.append("-f")
        psql_parm_list.append(sql_file.path)

        psql_parm_list.append(Const.V_ARG)
        psql_parm_list.append("ON_ERROR_STOP=1")

        if break_on_error:
            VersionDbShellUtil._execute_sql_on_db(
                psql, psql_parm_list, sql_file
            )
            return False

        try:
            VersionDbShellUtil._execute_sql_on_db(
                psql, psql_parm_list, sql_file
            )
        except VersionedDbExceptionSqlExecutionError as e:
            error_message_non_terminating(f"SQL ERROR {e.message}")
            VersionDbShellUtil._attempt_rollback(db_conn, sql_file)

    @staticmethod
    def _attempt_rollback(db_conn, sql_file):
        warning_message("Attempting to rollback")
        file_path = f"{sql_file.path[:-4]}{ROLLBACK_FILE_ENDING}"
        rollback = SqlPatch(file_path)
        VersionDbShellUtil.apply_sql_file(
            db_conn, rollback, break_on_error=True
        )

    @staticmethod
    def apply_data_sql_file(db_conn, sql_file, force=None,):
        psql = _local_psql()
        psql_parm_list = copy.copy(db_conn)

        psql_parm_list.append("-f")
        psql_parm_list.append(sql_file.path)

        if force is not True:
            psql_parm_list.append(Const.V_ARG)
            psql_parm_list.append("ON_ERROR_STOP=1")

        information_message(Const.PUSHING_DATA)
        VersionDbShellUtil._execute_sql_on_db(psql, psql_parm_list, sql_file)

    @staticmethod
    def _execute_sql_on_db(psql, psql_parm_list, sql_file):
        try:
            information_message(f"Running: {sql_file.fullname}")
            rtn = psql.run(psql_parm_list, retcode=0)
            msg = rtn[2]
            msg_formatted = msg.replace(f"psql:{sql_file.path}:", "\t")
            information_message(f"{msg_formatted}")

        except ProcessExecutionError as e:
            raise VersionedDbExceptionSqlExecutionError(e.stderr)

    @staticmethod
    def set_db_instance_version(
        db_conn,
        v_stg,
        new_version,
        new_hash=None,
        increase_rev=False,
        reset_rev=False,
    ):
        psql = _local_psql()
        rev_sql = ""

        hash_val = "Null" if new_hash is None else f"'{json.dumps(new_hash)}'"
        if increase_rev:
            rev_sql = f"{v_stg.rev} = {v_stg.rev} + 1,"

        if reset_rev:
            rev_sql = f"{v_stg.rev} = 0,"

        update_version_sql = f"UPDATE {v_stg.tbl} " f"SET {v_stg.v} = '{new_version}'," f"{rev_sql}" f" {v_stg.hash} = {hash_val};"
        psql(db_conn, "-c", update_version_sql, retcode=0)

    @staticmethod
    def dump_version_fast_forward(db_conn, v_stg, repo_name):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()
        conf.check_include_exclude_violation(repo_name)

        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        ensure_dir_exists(conf.fast_forward_dir())

        repo_ff = os.path.join(conf.fast_forward_dir(), dbver.repo_name)

        ensure_dir_exists(repo_ff)

        ff = os.path.join(repo_ff, f"{dbver.version}.sql")

        schema_args, tbl_args = _get_schema_table_args(conf, repo_name)

        pg_dump(db_conn, "-s", "-f", ff, schema_args, tbl_args, retcode=0)

        rtn = pg_dump(db_conn, "--column-inserts", "-a", "-t", v_stg.tbl)

        with open(ff, "a") as file:  # Use file to refer to the file object
            file.write(rtn)

        return True

    @staticmethod
    def dump_version_snapshot(db_conn, v_stg):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()

        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        ensure_dir_exists(conf.snapshot_dir())

        repo_sh = os.path.join(conf.snapshot_dir(), dbver.repo_name)

        ensure_dir_exists(repo_sh)

        d = datetime.datetime.now().strftime(SNAPSHOT_DATE_FORMAT)

        ss = os.path.join(repo_sh, f"{dbver.version}.{d}.sql")

        pg_dump(db_conn, "-s", "-f", ss, retcode=0)

    @staticmethod
    def dump_database_backup(db_conn, v_stg, dump_options: List[str]):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()

        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        ensure_dir_exists(conf.database_backup_dir())

        repo_db_bak = os.path.join(conf.database_backup_dir(), dbver.repo_name)

        ensure_dir_exists(repo_db_bak)

        d = datetime.datetime.now().strftime(SNAPSHOT_DATE_FORMAT)

        db_bak = os.path.join(repo_db_bak, f"{dbver.repo_name}.{d}.sql")

        schema_args, tbl_args = _get_schema_table_args(conf, dbver.repo_name)

        pg_dump(db_conn, dump_options, schema_args, tbl_args, "-f", db_bak, retcode=0)

    @staticmethod
    def display_db_instance_version(v_tbl, db_conn):
        dbv = VersionDbShellUtil.get_db_instance_version(v_tbl, db_conn)

        if dbv and dbv.version is None:
            error_message("No version found!")
        else:
            prod_display = " PRODUCTION" if dbv.is_production else ""
            env_display = (
                f" environment ({dbv.env})"
                if dbv.env
                else " environment (None)"
            )

            information_message(
                f"{dbv.version}.{dbv.revision}: {dbv.repo_name}{prod_display}{env_display}"
            )

    @staticmethod
    def get_db_instance_version(v_tbl, db_conn):
        psql = _local_psql()

        _good_version_table(v_tbl, db_conn)

        version_sql = f"SELECT {v_tbl.v}, {v_tbl.rev}, {v_tbl.repo}, {v_tbl.is_prod}, {v_tbl.env}, {v_tbl.hash}, " f"'notused' " f"throwaway FROM {v_tbl.tbl};"

        rtn = psql(db_conn, Const.TBL_ARG, "-A", "-c", version_sql, retcode=0)
        rtn_array = rtn.split("|")

        if len(rtn_array) > 1:
            return DatabaseRepositoryVersion(
                version=convert_str_none_if_empty(rtn_array[0]),
                revision=int(rtn_array[1]),
                repo_name=rtn_array[2],
                is_production=convert_str_to_bool(rtn_array[3]),
                env=convert_str_none_if_empty(rtn_array[4]),
                version_hash=rtn_array[5],
            )
        else:
            return None

    @staticmethod
    def get_col_inserts_setting(repo_name, tbl_name):
        conf = VersionDbShellUtil.get_data_dump_dict(repo_name)
        x = [tbl for tbl in conf if tbl[Const.DATA_TABLE] == tbl_name]
        if len(x) == 1:
            return x[0][Const.DATA_COLUMN_INSERTS]
        elif len(x) > 1:
            raise VersionedDbExceptionBadDataConfigFile()

        VersionDbShellUtil.add_col_inserts_setting(repo_name, tbl_name, True)
        return True

    @staticmethod
    def add_col_inserts_setting(repo_name, tbl_name, value):
        conf = VersionDbShellUtil.get_data_dump_dict(repo_name)
        conf_file = VersionDbShellUtil._get_data_dump_config_file(repo_name)

        x = [tbl for tbl in conf if tbl[Const.DATA_TABLE] == tbl_name]

        if len(x) == 0:
            conf.append({
                Const.DATA_TABLE: tbl_name,
                Const.DATA_COLUMN_INSERTS: value,
                Const.DATA_APPLY_ORDER: 0
            })
        else:
            x[0][Const.DATA_COLUMN_INSERTS] = value

        with open(conf_file, "w") as f:
            str_ = json.dumps(
                conf,
                indent=4,
                sort_keys=True,
                separators=(",", ": "),
                ensure_ascii=True,
            )
            f.write(to_unicode(str_))

    @staticmethod
    def get_data_dump_dict(repo_name):
        d = None
        conf_file = VersionDbShellUtil._get_data_dump_config_file(repo_name)

        if not os.path.isfile(conf_file):
            conf = RepositoryConf()
            ensure_dir_exists(conf.get_data_dump_dir(repo_name))
            make_data_file(conf_file)

        try:
            with open(conf_file) as json_data:
                d = json.load(json_data)
        except JSONDecodeError:
            raise VersionedDbExceptionBadDataConfigFile()

        return d

    @staticmethod
    def _get_data_dump_config_file(repo_name):
        conf = RepositoryConf()

        return os.path.join(
            conf.get_data_dump_dir(repo_name), DATA_DUMP_CONFIG_NAME
        )


def _build_arg_list(args: List[str], arg_type: str) -> Union[List[str], None]:
    arg_list = []

    if args:
        for i in args:
            arg_list.append(arg_type)
            arg_list.append(i)

    return arg_list


def _good_version_table(v_tbl, db_conn):
    good_table = False
    psql = _local_psql()

    version_sql = f"SELECT COUNT(*) cnt, 'notused' throwaway FROM {v_tbl.tbl};"

    psql_parm_list = copy.copy(db_conn)

    psql_parm_list.append(Const.TBL_ARG)
    psql_parm_list.append("-A")
    psql_parm_list.append("-c")
    psql_parm_list.append(version_sql)

    try:
        rtn = psql.run(psql_parm_list, retcode=0)
    except ProcessExecutionError as e:
        if e.retcode == 1:
            raise VersionedDbExceptionMissingVersionTable(v_tbl.tbl)
        elif e.retcode == 2:
            raise VersionedDbExceptionBadDateSource(db_conn)
        elif e.retcode > 2:
            raise VersionedDbException(e.stderr)

    rtn_array = rtn[STDOUT].split("|")
    count = int(rtn_array[0])

    if count == 1:
        good_table = True
    elif count == 0:
        raise VersionedDbExceptionNoVersionFound()
    elif count > 1:
        raise VersionedDbExceptionTooManyVersionRecordsFound()

    return good_table


def _get_schema_table_args(conf, repo_name: str) -> (List[str], List[str]):
    inc_sch = conf.get_repo_include_schemas(repo_name)
    exc_sch = conf.get_repo_exclude_schemas(repo_name)

    inc_tbl = conf.get_repo_list(
            repo_name=repo_name, list_name=INCLUDE_TABLES_PROP
    )
    exc_tbl = conf.get_repo_list(
            repo_name=repo_name, list_name=EXCLUDE_TABLES_PROP
    )

    tbl_args = []
    schema_args = []

    if inc_sch:
        schema_args = _build_arg_list(inc_sch, Const.PG_INCLUDE_SCHEMA_ARG)

    if exc_sch:
        schema_args = _build_arg_list(exc_sch, Const.PG_EXCLUDE_SCHEMA_ARG)

    if inc_tbl:
        tbl_args = _build_arg_list(inc_tbl, Const.PG_INCLUDE_TABLE_ARG)

    if exc_tbl:
        tbl_args = _build_arg_list(exc_tbl, Const.PG_EXCLUDE_TABLE_ARG)

    return schema_args, tbl_args


def get_table_size(tbl, db_conn):
    psql = _local_psql()

    tbl_sql = f"SELECT pg_size_pretty(pg_total_relation_size('{tbl['table']}')) " f"As tbl_size, pg_total_relation_size('{tbl['table']}') num_size;"

    rtn = psql[db_conn, Const.TBL_ARG, "-A", "-c", tbl_sql].run()

    rtn_array = rtn[STDOUT].split("|")
    size_txt = rtn_array[0]
    size_num = int(rtn_array[1])

    return size_num, size_txt


def convert_str_to_bool(value):
    if value is None:
        return None

    val = value.lower()
    if val == "t" or val == "true":
        return True
    elif val == "f" or val == "false":
        return False
    else:
        raise ValueError(f"convert_str_to_bool: invalid values {value}")


def convert_str_none_if_empty(value):
    return value if value else None


def warning_message(message):
    print(colors.yellow & colors.bold | message)


def error_message(message):
    print(colors.red & colors.bold | message)
    sys.exit(1)


def error_message_non_terminating(message):
    print(colors.red & colors.bold | message)


def information_message(message):
    print(colors.blue & colors.bold | message)


def notice_message(msg):
    print(colors.green & colors.bold | msg)


def repo_version_information_message(version, env):
    print(colors.blue & colors.bold | version, colors.green & colors.bold | env)


def sql_rollback_information_message(sql_message: str):
    print(colors.blue & colors.bold | sql_message, colors.green & colors.bold | "ROLLBACK")


def repo_unregistered_message(repo_name):
    print(
        colors.blue & colors.bold | repo_name,
        colors.red & colors.bold | "UNREGISTERED",
    )


def _local_psql():
    return local["psql"]


def _local_pg_dump():
    return local["pg_dump"]


def _local_pg_restore():
    return local["pg_restore"]
