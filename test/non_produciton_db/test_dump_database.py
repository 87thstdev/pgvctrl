from datetime import datetime
from unittest import mock

import dbversioning.dbvctrlConst as Const
from dbversioning.versionedDbShellUtil import SNAPSHOT_DATE_FORMAT
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out)


class TestDataDump:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_backups_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_data_dump_all(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == f"{TestUtil.pgvctrl_std_dump_reply}" \
                f"Repository {TestUtil.pgvctrl_test_repo} database backed up\n"
            assert errors is None

        files = TestUtil.get_backup_file_name(TestUtil.pgvctrl_test_repo)
        backup_file = files[0]
        date_str = backup_file.split(".")[1]
        date_of = datetime.strptime(date_str, SNAPSHOT_DATE_FORMAT)
        test_file_name = f"{TestUtil.pgvctrl_test_repo}.{date_str}"

        assert backup_file is not None
        assert type(date_of) is datetime
        assert test_file_name == backup_file

    def test_data_dump_cancel(self):
        with mock.patch('builtins.input', return_value="NO"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == TestUtil.pgvctrl_std_dump_cancelled_reply
            assert errors.code == 1

    def test_data_dump_cancel_any(self):
        with mock.patch('builtins.input', return_value="asdfsdfasdf"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == TestUtil.pgvctrl_std_dump_cancelled_reply
            assert errors.code == 1

    def test_data_dump_all_include(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == f"{TestUtil.pgvctrl_std_dump_reply}" \
                f"Repository {TestUtil.pgvctrl_test_repo} database backed up\n"
            assert errors is None

        files = TestUtil.get_backup_file_name(TestUtil.pgvctrl_test_repo)
        backup_file = files[0]

        has_member_sch = TestUtil.file_contains(
                f"{TestUtil.pgvctrl_test_db_backups_path}/{backup_file}",
                f"CREATE SCHEMA {TestUtil.schema_membership}",
        )
        has_public_sch = TestUtil.file_contains(
                f"{TestUtil.pgvctrl_test_db_backups_path}/{backup_file}",
                f"CREATE SCHEMA {TestUtil.schema_public}",
        )
        assert has_member_sch is True
        assert has_public_sch is False

    def test_data_dump_include_schema_bad(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_bad,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == f"{TestUtil.pgvctrl_std_dump_reply}DB Error pg_dump: no matching schemas were found\n\n"
            assert errors.code == 1

    def test_data_dump_exclude_schema(self):
        capture_dbvctrl_out(arg_list=[
            Const.EXCLUDE_SCHEMA_LONG_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == f"{TestUtil.pgvctrl_std_dump_reply}" \
                f"Repository {TestUtil.pgvctrl_test_repo} database backed up\n"
            assert errors is None

        files = TestUtil.get_backup_file_name(TestUtil.pgvctrl_test_repo)
        backup_file = files[0]

        has_member_sch = TestUtil.file_contains(
                f"{TestUtil.pgvctrl_test_db_backups_path}/{backup_file}",
                f"CREATE SCHEMA {TestUtil.schema_membership}",
        )
        has_public_sch = TestUtil.file_contains(
                f"{TestUtil.pgvctrl_test_db_backups_path}/{backup_file}",
                f"CREATE SCHEMA {TestUtil.schema_public}",
        )
        assert has_member_sch is False
        assert has_public_sch is True


class TestDataDumpENv:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
        ])
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
            Const.SET_ENV_ARG,
            TestUtil.env_test,
        ])
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.ENV_ARG,
            TestUtil.env_test,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_backups_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_data_dump_env(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == f"{TestUtil.pgvctrl_std_dump_reply}" \
                f"Repository {TestUtil.pgvctrl_test_repo} database backed up\n"
            assert errors is None

        files = TestUtil.get_backup_file_name(TestUtil.pgvctrl_test_repo)
        backup_file = files[0]
        date_str = backup_file.split(".")[2]
        date_of = datetime.strptime(date_str, SNAPSHOT_DATE_FORMAT)
        test_file_name = f"{TestUtil.pgvctrl_test_repo}.{TestUtil.env_test}.{date_str}"

        assert type(date_of) is datetime
        assert test_file_name == backup_file
