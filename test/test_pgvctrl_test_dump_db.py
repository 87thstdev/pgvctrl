from datetime import datetime
from unittest import mock

import dbversioning.dbvctrlConst as Const
from dbversioning.versionedDbShellUtil import SNAPSHOT_DATE_FORMAT
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out)


class TestPgvctrlTestDbDataDump:
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
                Const.DUMP_DATABASE_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == "Do you want to dump the database? [YES/NO]\ndump-database\n"
            assert errors is None

        files = TestUtil.get_backup_file_name(TestUtil.pgvctrl_test_repo)
        backup_file = files[0]
        date_str = backup_file.split(".")[1]
        date_of = datetime.strptime(date_str, SNAPSHOT_DATE_FORMAT)
        test_file_name = f"{TestUtil.pgvctrl_test_repo}.{date_str}.sql"

        assert backup_file is not None
        assert type(date_of) is datetime
        assert test_file_name == backup_file

    def test_data_dump_cancel(self):
        with mock.patch('builtins.input', return_value="NO"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_DATABASE_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == "Do you want to dump the database? [YES/NO]\nDump database cancelled.\n"
            assert errors.code == 1

    def test_data_dump_cancel_any(self):
        with mock.patch('builtins.input', return_value="asdfsdfasdf"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_DATABASE_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == "Do you want to dump the database? [YES/NO]\nDump database cancelled.\n"
            assert errors.code == 1

    def test_data_dump_all_include(self):
        capture_dbvctrl_out(arg_list=[
            Const.INCLUDE_SCHEMA_ARG,
            TestUtil.schema_membership,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])

        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_DATABASE_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == "Do you want to dump the database? [YES/NO]\ndump-database\n"
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
