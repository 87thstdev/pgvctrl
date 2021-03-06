import mmap
import os
import io
import sys
from contextlib import redirect_stdout
from shutil import rmtree
from typing import List

from plumbum import local

import dbversioning.dbvctrlConst as Const
from dbversioning.osUtil import ensure_dir_exists
from dbversioning.versionedDbShellUtil import STDOUT

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dbversioning.dbvctrl import DbVctrl, parse_args

# NOTICE:
#   Only change these constants for your postgres environment if you need to for testing.
LOCAL_HOST = "localhost"
PORT = "5432"
PG_TEST_SERVICE = "pgvctrl_test"
PG_USER = "87admin"
PG_PSW = ""

#
# [pgvctrl_test:test]
#     host=localhost
#     port=5432
#     dbname=pgvctrl_test_db

# DON'T CHANGE LINES BELOW


class TestUtil(object):
    local_host_server = LOCAL_HOST
    port = PORT
    svc = PG_TEST_SERVICE
    user = PG_USER
    psw = PG_PSW
    user_bad = "BADUSER_asdfafasdfda"
    psw_bad = "BADPSW_aslkjfdoija"
    return_code = 0
    stdout = 1
    stderr = 2
    invalid_repo_name = "/////"
    pgvctrl_test_repo = "pgvctrl_test"
    pgvctrl_bad_repo = "BAD_REPO"
    pgvctrl_test_temp_repo = "pgvctrl_temp_test"
    pgvctrl_test_temp_repo_path = "databases/pgvctrl_temp_test"
    pgvctrl_no_files_repo = "pgvctrl_no_files"
    pgvctrl_no_files_repo_path = "databases/pgvctrl_no_files"
    pgvctrl_test_db = "pgvctrl_test_db"
    pgvctrl_std_dump_qa_reply = "Do you want to dump the database? [YES/NO]\n"
    pgvctrl_std_dump_reply = f"{pgvctrl_std_dump_qa_reply}"
    pgvctrl_std_dump_cancelled_reply = f"{pgvctrl_std_dump_qa_reply}Dump database cancelled.\n"
    db_backups_path = (
        f"databases/_databaseBackup/"
    )
    pgvctrl_test_db_backups_path = (
        f"{db_backups_path}{pgvctrl_test_repo}"
    )
    pgvctrl_std_restore_qa_reply = "Do you want to restore the database? [YES/NO]\n"
    pgvctrl_std_restore_reply = f"{pgvctrl_std_restore_qa_reply}"
    pgvctrl_std_restore_cancelled_reply = (
        f"{pgvctrl_std_restore_qa_reply}Restore database cancelled.\n"
    )
    pgvctrl_std_restore_none_empty_db_reply = (
        f"{pgvctrl_std_restore_qa_reply}Database restores only allowed on empty databases.\n"
    )
    test_bad_version_number = "one.0.0.first"
    test_first_version = "0.0.0.first"
    test_first_version_path = (
        f"databases/{pgvctrl_test_repo}/{test_first_version}"
    )
    test_version_no_name = "1.0.0"
    test_second_version_no_name = "1.0.0"
    test_version = "2.0.0.NewVersion"
    pgvctrl_databases_ss_path = (
        f"databases/_schemaSnapshot"
    )
    pgvctrl_test_db_ss_path = (
        f"databases/_schemaSnapshot/{pgvctrl_test_repo}"
    )
    test_version_ss_path = (
        f"{pgvctrl_test_db_ss_path}/{test_version}.sql"
    )
    test_version_data_path = f"databases/{pgvctrl_test_repo}/data/data.json"
    test_make_version = "3.0.0.MakeNewVersion"
    test_make_version_path = (
        f"databases/{pgvctrl_test_repo}/{test_make_version}"
    )
    test_bad_version = "999.1.bad_version"
    test_temp_version_path = f"databases/pgvctrl_temp_test/{test_version}"
    test_temp_version_no_name_path = f"databases/pgvctrl_temp_test/{test_version_no_name}"
    test_sql_path = f"databases/pgvctrl_test/{test_version}/"
    error_sql = "130.Error.sql"
    error_sql_path = f"databases/pgvctrl_test/{test_version}/{error_sql}"
    error_sql_rollback = "130.Error_rollback.sql"
    error_sql_rollback_path = (
        f"databases/pgvctrl_test/{test_version}/{error_sql_rollback}"
    )
    env_test = "test"
    env_qa = "qa"
    env_prod = "prod"
    version_table_owner = "test_owner"
    schema_membership = "membership"
    schema_public = "public"
    schema_bad = "badschemaname"
    table_membership_user_state = f"{schema_membership}.user_state"
    table_public_item = f"{schema_public}.item"
    table_bad = "badtablename"
    error_set_table_name = "error_set"
    error_set_table_error_set_table = '{\n        "apply-order": 0,\n        "column-inserts": true,\n        "table": "error_set"\n    }'
    bad_table_name = "asdeeiaoivjaiosdj"
    custom_error_message = "Custom General Error"
    bad_sql_name = "one.bad_number.sql"
    error_set_data_folder_path = f"databases/{pgvctrl_test_repo}/data"
    app_error_set_data_path = f"{error_set_data_folder_path}/app_error_set.sql"
    error_set_data_path = f"{error_set_data_folder_path}/error_set.sql"
    config_file = "dbRepoConfig.json"
    database_folder = "databases"
    data_file_default = "data.json.default"
    data_file_applying_default = "data.json.applying.default"
    app_error_set_file_default = "app_error_set.sql.default"
    error_set_file_default = "error_set.sql.default"
    test_version_pre_push_path = f"databases/{pgvctrl_test_repo}/data/{Const.DATA_PRE_PUSH_FILE}"
    test_version_post_push_path = f"databases/{pgvctrl_test_repo}/data/{Const.DATA_POST_PUSH_FILE}"

    @staticmethod
    def local_psql():
        return local["psql"]

    @staticmethod
    def create_database():
        TestUtil.drop_database()

        psql = TestUtil.local_psql()
        rtn = psql.run(
            ["-h", "localhost", "-c", f"CREATE DATABASE {TestUtil.pgvctrl_test_db}"], retcode=0
        )
        print(rtn)

    @staticmethod
    def create_table_owner_role():
        psql = TestUtil.local_psql()
        rtn = psql.run(
                ["-h", "localhost", "-c", f"CREATE ROLE {TestUtil.version_table_owner};"],
                retcode=(0, 1)
        )
        print(rtn)

    @staticmethod
    def remove_table_owner_role():
        psql = TestUtil.local_psql()
        rtn = psql.run(
                ["-h", "localhost", "-c", f"DROP ROLE {TestUtil.version_table_owner};"],
                retcode=(0, 1)
        )
        print(rtn)

    @staticmethod
    def get_table_owner(db_name: str, table_name: str):
        psql = TestUtil.local_psql()
        rtn = psql.run(
                ["-h", "localhost", "-d",
                 db_name,
                 "-A",
                 "-c",
                 f"SELECT tableowner FROM pg_tables where tablename = '{table_name}';"],
                retcode=0
        )
        rtn_array = rtn[STDOUT].split("|")
        owner = rtn_array[0].split("\n")[1]

        print(owner)
        return owner

    @staticmethod
    def drop_database():
        psql = TestUtil.local_psql()
        rtn = psql.run(
            ["-h", "localhost", "-c", f"DROP DATABASE IF EXISTS {TestUtil.pgvctrl_test_db}"],
            retcode=0,
        )
        print(rtn)

    @staticmethod
    def remove_rev_recs(db_name: str):
        psql = TestUtil.local_psql()
        psql.run(
                ["-h", "localhost", "-d",
                 db_name,
                 "-A",
                 "-c",
                 "DELETE FROM repository_version;"],
                retcode=0
        )

    @staticmethod
    def add_rev_recs(db_name: str):
        psql = TestUtil.local_psql()
        psql.run(
                ["-h", "localhost", "-d",
                 db_name,
                 "-A",
                 "-c",
                 "INSERT INTO repository_version (repository_name, is_production, env) VALUES ('test', false, 'test');"],
                retcode=0
        )

    @staticmethod
    def delete_file(file_name):
        if os.path.isfile(file_name):
            os.remove(file_name)

    @staticmethod
    def delete_folder(dir_name):
        if os.path.isdir(dir_name):
            os.rmdir(dir_name)

    @staticmethod
    def delete_folder_full(dir_name):
        if os.path.isdir(dir_name):
            rmtree(dir_name)

    @staticmethod
    def create_config(contents: str = None):
        if contents is None:
            out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.MKCONF_ARG])
            print(out_rtn)
        else:
            with open(TestUtil.config_file, 'w') as f:
                f.write(contents)

    @staticmethod
    def remove_config():
        TestUtil.delete_file(TestUtil.config_file)

    @staticmethod
    def mkrepo(repo_name):
        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.MAKE_REPO_ARG, repo_name])
        print(out_rtn)

    @staticmethod
    def mkrepo_ver(repo_name, version):
        out_rtn, errors = capture_dbvctrl_out(arg_list=[Const.REPO_ARG, repo_name, Const.MAKE_V_ARG, version])
        assert errors is None
        print(out_rtn)

    @staticmethod
    def make_conf():
        capture_dbvctrl_out(arg_list=[
            Const.MKCONF_ARG
        ])

    @staticmethod
    def create_simple_data_sql_file(repo_name: str, file_name: str, contents: str = None):
        ensure_dir_exists(f"databases/{repo_name}/data")
        full_file_name = f'databases/{repo_name}/data/{file_name}'
        if not os.path.exists(full_file_name):
            with open(full_file_name, 'w') as f:
                if contents:
                    f.write(contents)
                else:
                    f.write(f"SELECT '{file_name}' as file_name;\n")

    @staticmethod
    def create_simple_sql_file(
            repo_name: str, version: str, file_name: str, contents: str = None, overwrite: bool = False):
        ensure_dir_exists(f'databases/{repo_name}/{version}')
        full_file_name = f'databases/{repo_name}/{version}/{file_name}'
        if overwrite or not os.path.exists(full_file_name):
            with open(full_file_name, 'w') as f:
                if contents:
                    f.write(contents)
                else:
                    f.write(f"SELECT '{file_name}' as file_name;\n")
        else:
            raise Exception(f'File present {full_file_name}')

    @staticmethod
    def create_data_applying_config(repo_name: str, contents: str = None):
        ensure_dir_exists(f'databases/{repo_name}/data')
        full_file_name = f'databases/{repo_name}/data/data.json'
        if not os.path.exists(full_file_name):
            with open(full_file_name, 'w') as f:
                if contents:
                    f.write(contents)
                else:
                    f.write("[]")

    @staticmethod
    def create_repo_ss_sql_file(repo_name: str, file_names: List[str]):
        ensure_dir_exists(f"databases/_schemaSnapshot/{repo_name}")
        for file_name in file_names:
            full_file_name = f'databases/_schemaSnapshot/{repo_name}/{file_name}'
            if not os.path.exists(full_file_name):
                with open(full_file_name, 'w'):
                    pass

    @staticmethod
    def create_repo_dd_file(repo_name: str, file_name: str):
        ensure_dir_exists(f"databases/_databaseBackup/{repo_name}")
        full_file_name = f'databases/_databaseBackup/{repo_name}/{file_name}'
        if not os.path.exists(full_file_name):
            with open(full_file_name, 'w'):
                pass

    @staticmethod
    def create_root_folder():
        ensure_dir_exists(TestUtil.database_folder)

    @staticmethod
    def remove_root_folder():
        TestUtil.delete_folder_full(TestUtil.database_folder)

    @staticmethod
    def create_repo_folder(repo_name: str):
        ensure_dir_exists(f"{TestUtil.database_folder}/{repo_name}")

    @staticmethod
    def get_backup_file_name(repo: str):
        return os.listdir(f"databases/_databaseBackup/{repo}")

    @staticmethod
    def get_snapshot_file_names(repo: str):
        return os.listdir(f"databases/_schemaSnapshot/{repo}")

    @staticmethod
    def file_contains(file_path, key):
        with open(file_path, "rb", 0) as file, mmap.mmap(
            file.fileno(), 0, access=mmap.ACCESS_READ
        ) as s:
            if s.find(key.encode()) != -1:
                return True

        return False


def capture_dbvctrl_out(arg_list: List[str]):
    out = io.StringIO()
    errors = None
    with redirect_stdout(out):
        try:
            args = parse_args(arg_list)
            DbVctrl.run(args)
        except BaseException as e:
            errors = e

    return out.getvalue(), errors


def print_cmd_error_details(rtn, arg_list):
    print(f":pgvctrl {' '.join(arg_list)}")
    print(f"return: {rtn}")


def dbvctrl_assert_simple_msg(arg_list: List[str], msg: str, error_code: int=None):
    out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

    print_cmd_error_details(out_rtn, arg_list)

    assert out_rtn == msg
    if error_code:
        assert errors.code == error_code
    else:
        assert errors is None
