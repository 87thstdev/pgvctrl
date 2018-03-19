import os

import datetime
import simplejson as json

import copy
from plumbum import (
    colors,
    local,
    ProcessExecutionError)
from simplejson import JSONDecodeError

from dbversioning.osUtil import (
    ensure_dir_exists,
    make_data_file)
from dbversioning.versionedDb import SqlPatch
from dbversioning.errorUtil import (
    VersionedDbExceptionMissingVersionTable,
    VersionedDbExceptionBadDateSource,
    VersionedDbExceptionNoVersionFound,
    VersionedDbException,
    VersionedDbExceptionTooManyVersionRecordsFound,
    VersionedDbExceptionDatabaseAlreadyInit,
    VersionedDbExceptionSqlExecutionError,
    VersionedDbExceptionBadDataConfigFile,
    VersionedDbExceptionMissingDataTable,
)
from dbversioning.repositoryconf import (
    RepositoryConf,
    ROLLBACK_FILE_ENDING)

DATA_DUMP_CONFIG_NAME = 'data.json'
RETCODE = 0
STDOUT = 1
STDERR = 2

SNAPSHOT_DATE_FORMAT = '%Y%m%d%H%M%S'

to_unicode = str


class DatabaseRepositoryVersion(object):
    def __init__(self, version=None, repo_name=None, is_production=None, version_hash=None):
        self.version = version
        self.repo_name = repo_name
        self.is_production = is_production
        self.version_hash = version_hash


class VersionDbShellUtil:
    @staticmethod
    def __init__():
        pass

    @staticmethod
    def init_db(repo_name, v_stg=None, db_conn=None, is_production=False):
        psql = _local_psql()
        conf = RepositoryConf()
        missing_tbl = False
        need_version = False
        already_init = False
        default_version = "0.0.gettingStarted"

        try:
            dbv = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)
            if dbv:
                already_init = True
                warning_message("Already initialized")
                return False

        except VersionedDbExceptionMissingVersionTable:
            missing_tbl = True
        except VersionedDbExceptionNoVersionFound:
            need_version = True
        except VersionedDbExceptionDatabaseAlreadyInit as e:
            error_message(e.message)
            return False

        create_v_tbl = "CREATE TABLE IF NOT EXISTS {tbl} (" \
                       "{v} VARCHAR NOT NULL," \
                       "{repo} VARCHAR NOT NULL," \
                       "{is_prod} BOOLEAN NOT NULL," \
                       "{hash} JSONB);" \
            .format(tbl=v_stg.tbl, v=v_stg.v, is_prod=v_stg.is_prod ,hash=v_stg.hash, repo=v_stg.repo)

        insert_v_sql = "INSERT INTO {tbl} ({repo}, {v}, {is_prod}, {hash}) " \
                       "VALUES ('{repo_name}', '{ver_num}', '{ver_is_prod}', '{ver_hash}');" \
            .format(
                tbl=v_stg.tbl,
                repo=v_stg.repo,
                v=v_stg.v,
                is_prod=v_stg.is_prod,
                hash=v_stg.hash,
                ver_hash='null',
                ver_is_prod=is_production,
                repo_name=repo_name,
                ver_num=default_version
            )
        if missing_tbl:
            psql(db_conn, "-c", create_v_tbl, retcode=0)

        if already_init:
            VersionDbShellUtil.set_db_instance_version(db_conn, v_stg, default_version)
        else:
            psql(db_conn, "-c", insert_v_sql, retcode=0)

        ensure_dir_exists(os.path.join(conf.root(), repo_name))
        ensure_dir_exists(os.path.join(conf.root(), repo_name, default_version))

        return True

        # TODO: Decided if I want to create a ff point on init.
        # DbVersionShellHelper.dump_version_fast_forward(db_conn, v_stg)

    @staticmethod
    def apply_fast_forward_sql(db_conn, sql_file, repo_name):
        psql = _local_psql()
        psql_parm_list = copy.copy(db_conn)

        psql_parm_list.append("-f")
        psql_parm_list.append(sql_file.sql_file)
        psql_parm_list.append("-v")
        psql_parm_list.append("ON_ERROR_STOP=1")
        try:
            information_message("Applying: {0}".format(sql_file.full_name))
            rtn = psql.run(psql_parm_list, retcode=0)
            msg = rtn[2]
            msg_formatted = msg.replace("psql:{0}:".format(sql_file.full_name), "\t")
            information_message("{0}".format(msg_formatted))

            # TODO: Make this happen
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
            sql_loc = conf.get_data_dump_sql_dir(repo_name, "{0}.sql".format(tbl))

            pg_dump_parm_list = copy.copy(db_conn)
            if VersionDbShellUtil.get_col_inserts_setting(repo_name, tbl):
                pg_dump_parm_list.append("--column-inserts")
            pg_dump_parm_list.append("--disable-triggers")
            pg_dump_parm_list.append("--if-exists")
            pg_dump_parm_list.append("-c")
            pg_dump_parm_list.append("-t")
            pg_dump_parm_list.append(tbl)
            pg_dump_parm_list.append("-f")
            pg_dump_parm_list.append(sql_loc)

            try:
                information_message("Pulling: {0}".format(tbl))
                pg_dump(pg_dump_parm_list, retcode=0)
            except ProcessExecutionError as e:
                raise VersionedDbExceptionSqlExecutionError(e.stderr)

    @staticmethod
    def pull_repo_tables_from_database(repo_name, db_conn):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()

        ensure_dir_exists(conf.get_data_dump_dir(repo_name))
        table_list = VersionDbShellUtil._get_data_dump_dict(repo_name)

        if len(table_list) == 0:
            information_message("No tables to pull!")
            return

        for tbl in table_list:
            sql_loc = conf.get_data_dump_sql_dir(repo_name, "{0}.sql".format(tbl['table']))

            pg_dump_parm_list = copy.copy(db_conn)
            if tbl['column-inserts']:
                pg_dump_parm_list.append("--column-inserts")
            pg_dump_parm_list.append("--disable-triggers")
            pg_dump_parm_list.append("--if-exists")
            pg_dump_parm_list.append("-c")
            pg_dump_parm_list.append("-t")
            pg_dump_parm_list.append(tbl['table'])
            pg_dump_parm_list.append("-f")
            pg_dump_parm_list.append(sql_loc)

            try:
                size_num, size_txt = get_table_size(tbl, db_conn)
                information_message("Pulling: {0}, {1}".format(tbl['table'], size_txt))
                pg_dump(pg_dump_parm_list, retcode=0)
            except ProcessExecutionError as e:
                raise VersionedDbExceptionSqlExecutionError(e.stderr)

    @staticmethod
    def apply_sql_file(db_conn, sql_file, break_on_error=False):
        psql = _local_psql()
        psql_parm_list = copy.copy(db_conn)

        psql_parm_list.append("-f")
        psql_parm_list.append(sql_file.path)

        psql_parm_list.append("-v")
        psql_parm_list.append("ON_ERROR_STOP=1")

        if break_on_error:
            VersionDbShellUtil._execute_sql_on_db(psql, psql_parm_list, sql_file)
            return False

        try:
            VersionDbShellUtil._execute_sql_on_db(psql, psql_parm_list, sql_file)
        except VersionedDbExceptionSqlExecutionError as e:
            error_message("SQL ERROR {0}".format(e.message))
            VersionDbShellUtil._attempt_rollback(db_conn, sql_file)

    @staticmethod
    def _attempt_rollback(db_conn, sql_file):
        warning_message("Attempting to rollback")
        file_path = "{0}{1}".format(sql_file.path[:-4], ROLLBACK_FILE_ENDING)
        rollback = SqlPatch(file_path)
        VersionDbShellUtil.apply_sql_file(db_conn, rollback, break_on_error=True)

    @staticmethod
    def apply_data_sql_file(db_conn, sql_file, force=None):
        psql = _local_psql()
        psql_parm_list = copy.copy(db_conn)

        psql_parm_list.append("-f")
        psql_parm_list.append(sql_file.path)

        if force is not True:
            psql_parm_list.append("-v")
            psql_parm_list.append("ON_ERROR_STOP=1")

        VersionDbShellUtil._execute_sql_on_db(psql, psql_parm_list, sql_file)

    @staticmethod
    def _execute_sql_on_db(psql, psql_parm_list, sql_file):
        try:
            information_message("Running: {0}".format(sql_file.fullname))
            rtn = psql.run(psql_parm_list, retcode=0)
            msg = rtn[2]
            msg_formatted = msg.replace("psql:{0}:".format(sql_file.path), "\t")
            information_message("{0}".format(msg_formatted))

        except ProcessExecutionError as e:
            raise VersionedDbExceptionSqlExecutionError(e.stderr)

    @staticmethod
    def set_db_instance_version(db_conn, v_stg, new_version, new_hash=None):
        psql = _local_psql()
        hash_val = 'Null' if new_hash is None else "'{0}'".format(json.dumps(new_hash))
        update_version_sql = "UPDATE {table} " \
                             "SET {v} = '{ver}'," \
                             " {ver_hash} = {hash};" \
            .format(
                table=v_stg.tbl,
                v=v_stg.v,
                ver=new_version,
                ver_hash=v_stg.hash,
                hash=hash_val
            )

        psql(db_conn, "-c", update_version_sql, retcode=0)

    @staticmethod
    def dump_version_fast_forward(db_conn, v_stg):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()

        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        ensure_dir_exists(conf.fast_forward_dir())

        repo_ff = os.path.join(conf.fast_forward_dir(), dbver.repo_name)

        ensure_dir_exists(repo_ff)

        ff = os.path.join(repo_ff, "{0}.sql".format(dbver.version))

        pg_dump(db_conn, "-s", "-f", ff, retcode=0)

    @staticmethod
    def dump_version_snapshot(db_conn, v_stg):
        pg_dump = _local_pg_dump()
        conf = RepositoryConf()

        dbver = VersionDbShellUtil.get_db_instance_version(v_stg, db_conn)

        ensure_dir_exists(conf.snapshot_dir())

        repo_sh = os.path.join(conf.snapshot_dir(), dbver.repo_name)

        ensure_dir_exists(repo_sh)

        d = datetime.datetime.now().strftime(SNAPSHOT_DATE_FORMAT)

        ss = os.path.join(repo_sh, "{0}.{1}.sql"
                          .format(dbver.version, d))

        pg_dump(db_conn, "-s", "-f", ss, retcode=0)

    @staticmethod
    def display_db_instance_version(v_tbl, db_conn):
        dbv = VersionDbShellUtil.get_db_instance_version(v_tbl, db_conn)

        if dbv and dbv.version is None:
            warning_message("No version found!")
        else:
            if dbv.is_production:
                information_message("{0}: {1} PRODUCTION".format(dbv.version, dbv.repo_name))
            else:
                information_message("{0}: {1}".format(dbv.version, dbv.repo_name))

    @staticmethod
    def get_db_instance_version(v_tbl, db_conn):
        psql = _local_psql()

        _good_version_table(v_tbl, db_conn)

        version_sql = "SELECT {v}, {repo}, {is_prod}, {hash}, 'notused' throwaway FROM {tbl};"\
            .format(repo=v_tbl.repo, v=v_tbl.v, is_prod=v_tbl.is_prod, hash=v_tbl.hash, tbl=v_tbl.tbl)

        rtn = psql(db_conn, "-t", "-A", "-c", version_sql, retcode=0)
        rtn_array = rtn.split("|")

        if len(rtn_array) > 1:
            return DatabaseRepositoryVersion(
                version=rtn_array[0],
                repo_name=rtn_array[1],
                is_production=convert_str_to_bool(rtn_array[2]),
                version_hash=rtn_array[3]
            )
        else:
            return None

    @staticmethod
    def get_col_inserts_setting(repo_name, tbl_name):
        conf = VersionDbShellUtil._get_data_dump_dict(repo_name)
        x = [tbl for tbl in conf if tbl["table"] == tbl_name]
        if len(x) == 1:
            return x[0]["column-inserts"]
        elif len(x) > 1:
            raise VersionedDbExceptionBadDataConfigFile()

        VersionDbShellUtil.add_col_inserts_setting(repo_name, tbl_name, True)
        return True

    @staticmethod
    def add_col_inserts_setting(repo_name, tbl_name, value):
        conf = VersionDbShellUtil._get_data_dump_dict(repo_name)
        conf_file = VersionDbShellUtil._get_data_dump_config_file(repo_name)

        x = [tbl for tbl in conf if tbl["table"] == tbl_name]

        if len(x) == 0:
            conf.append({"table": tbl_name, "column-inserts": value})
        else:
            x[0]["column-inserts"] = value

        with open(conf_file, 'w') as f:
            str_ = json.dumps(conf,
                              indent=4, sort_keys=True,
                              separators=(',', ': '), ensure_ascii=True)
            f.write(to_unicode(str_))

    @staticmethod
    def _get_data_dump_dict(repo_name):
        d = None
        conf_file = VersionDbShellUtil._get_data_dump_config_file(repo_name)

        if not os.path.isfile(conf_file):
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

        return os.path.join(conf.get_data_dump_dir(repo_name), DATA_DUMP_CONFIG_NAME)


def _good_version_table(v_tbl, db_conn):
    good_table = False
    psql = _local_psql()

    version_sql = "SELECT COUNT(*) cnt, 'notused' throwaway FROM {tbl};" \
        .format(tbl=v_tbl.tbl)

    psql_parm_list = copy.copy(db_conn)

    psql_parm_list.append("-t")
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


def get_table_size(tbl, db_conn):
    psql = _local_psql()

    tbl_sql = "SELECT pg_size_pretty(pg_total_relation_size('{0}')) " \
        "As tbl_size, pg_total_relation_size('{0}') num_size;" \
        .format(tbl['table'])

    rtn = psql[db_conn, "-t", "-A", "-c", tbl_sql].run()
    if rtn[RETCODE] > 0:
        raise VersionedDbExceptionMissingDataTable(tbl['table'])

    rtn_array = rtn[STDOUT].split("|")
    size_txt = rtn_array[0]
    size_num = int(rtn_array[1])

    return size_num, size_txt


def convert_str_to_bool(value):
    if value is None:
        return None

    val = value.lower()
    if val == 't' or val == 'true':
        return True
    elif val == 'f' or val == 'false':
        return False
    else:
        raise ValueError("convert_str_to_bool: invalid valuse {0}".format(value))


def warning_message(message):
    print(colors.yellow & colors.bold | message)


def error_message(message):
    print(colors.red & colors.bold | message)


def information_message(message):
    print(colors.blue & colors.bold | message)


def _local_psql():
    return local["psql"]


def _local_pg_dump():
    return local["pg_dump"]


def _local_pg_restore():
    return local["pg_restore"]
