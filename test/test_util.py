import mmap
import os
import io
import sys
from contextlib import redirect_stdout
from shutil import copy2
import simplejson as json

from plumbum import local

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dbversioning.dbvctrl import DbVctrl


class TestUtil(object):
    return_code = 0
    stdout = 1
    stderr = 2
    pgvctrl_test_repo = "pgvctrl_test"
    pgvctrl_test_temp_repo = "pgvctrl_temp_test"
    pgvctrl_test_temp_repo_path = "databases/pgvctrl_temp_test"
    pgvctrl_no_files_repo = "pgvctrl_no_files"
    pgvctrl_no_files_repo_path = "databases/pgvctrl_no_files"
    pgvctrl_test_db = "pgvctrl_test_db"
    test_first_version = "0.0.0.first"
    test_first_version_path = f"databases/{pgvctrl_test_repo}/{test_first_version}"
    test_version = "2.0.0.NewVersion"
    test_version_ff_path = f"databases/_fastForward/{pgvctrl_test_repo}/{test_version}.sql"
    test_make_version = "3.0.0.MakeNewVersion"
    test_make_version_path = f"databases/{pgvctrl_test_repo}/{test_make_version}"
    test_bad_version = "999.1.bad_version"
    test_version_path = "databases/pgvctrl_temp_test/{0}".format(test_version)
    error_sql = '130.Error.sql'
    error_sql_path = f'databases/pgvctrl_test/{test_version}/{error_sql}'
    error_sql_rollback = '130.Error_rollback.sql'
    error_sql_rollback_path = f'databases/pgvctrl_test/{test_version}/{error_sql_rollback}'
    env_test = "test"
    env_qa = "qa"
    env_prod = "prod"
    schema_membership = "membership"
    schema_public = "public"
    schema_bad = "badschemaname"
    table_membership_user_state = f"{schema_membership}.user_state"
    table_public_item = f"{schema_public}.item"
    table_bad = "badtablename"
    config_file = 'dbRepoConfig.json'

    sql_return = 'Running: 100.AddUsersTable\n\n' \
                 'Running: 110.Notice\n' \
                 '\t8: NOTICE:  WHO DAT? 87admin\n' \
                 '\t8: NOTICE:  Just me, 87admin\n' \
                 '\t8: NOTICE:  Guess we are talking to ourselves!  87admin\n\n' \
                 'Running: 120.ItemTable\n\n' \
                 'Running: 140.ItemsAddMore\n\n' \
                 'Running: 200.AddEmailTable\n\n' \
                 'Running: 300.UserStateTable\n\n' \
                 'Running: 400.ErrorSet\n\n' \
                 f'Applied: {pgvctrl_test_repo} v {test_version}.0\n'

    @staticmethod
    def local_pgvctrl():
        return local["pgvctrl"]

    @staticmethod
    def local_psql():
        return local["psql"]
    
    @staticmethod
    def create_database():
        psql = TestUtil.local_psql()
        rtn = psql.run(["-c", "CREATE DATABASE {0}".format(TestUtil.pgvctrl_test_db)], retcode=0)
        print(rtn)

    @staticmethod
    def drop_database():
        psql = TestUtil.local_psql()
        rtn = psql.run(["-c", "DROP DATABASE IF EXISTS {0}".format(TestUtil.pgvctrl_test_db)], retcode=0)
        print(rtn)

    @staticmethod
    def delete_file(file_name):
        if os.path.isfile(file_name):
            os.remove(file_name)

    @staticmethod
    def delete_folder(dir_name):
        if os.path.isdir(dir_name):
            os.rmdir(dir_name)

    @staticmethod
    def create_config():
        pgv = TestUtil.local_pgvctrl()
        rtn = pgv.run(["-mkconf"], retcode=0)
        print(rtn)

    @staticmethod
    def mkrepo(repo_name):
        pgv = TestUtil.local_pgvctrl()
        rtn = pgv.run(["-mkrepo", repo_name], retcode=0)
        print(rtn)


    @staticmethod
    def mkrepo_ver(repo_name, ver):
        pgv = TestUtil.local_pgvctrl()
        rtn = pgv.run(["-repo", repo_name, '-mkv', ver], retcode=0)
        print(rtn)

    @staticmethod
    def get_static_config():
        copy2(f'{TestUtil.config_file}.default', TestUtil.config_file)

    @staticmethod
    def get_error_sql():
        copy2(f'{TestUtil.error_sql_path}.default', TestUtil.error_sql_path)

    @staticmethod
    def get_error_rollback_bad_sql():
        copy2(f'{TestUtil.error_sql_rollback_path}.bad.default', TestUtil.error_sql_rollback_path)

    @staticmethod
    def get_error_rollback_good_sql():
        copy2(f'{TestUtil.error_sql_rollback_path}.good.default', TestUtil.error_sql_rollback_path)

    @staticmethod
    def get_repo_dict():
        d = None
        if os.path.isfile(TestUtil.config_file):
            with open(TestUtil.config_file) as json_data:
                d = json.load(json_data)

        return d

    @staticmethod
    def file_contains(file_path, key):
        with open(file_path, 'rb', 0) as file, \
                mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
            if s.find(key.encode()) != -1:
                return True

        return False


def capture_dbvctrl_out(args):
    out = io.StringIO()
    errors = None
    with redirect_stdout(out):
        try:
            DbVctrl.run(args)
        except BaseException as e:
            errors = e

    return out.getvalue(), errors


def print_cmd_error_details(rtn, arg_list):
    print(":pgvctrl {0}".format(' '.join(arg_list)))
    print("return: {0}".format(rtn))
