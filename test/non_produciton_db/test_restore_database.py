from unittest import mock

import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestDatabaseRestore:
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
        with mock.patch('builtins.input', return_value="YES"):
            capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])

        files = TestUtil.get_backup_file_name(TestUtil.pgvctrl_test_repo)
        self.backup_file = files[0]

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_backups_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_database_restore(self):
        TestUtil.drop_database()
        TestUtil.create_database()

        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.RESTORE_ARG,
                self.backup_file,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == (
                f"{TestUtil.pgvctrl_std_restore_qa_reply}"
                f"Database {self.backup_file} "
                f"from repository pgvctrl_test restored ['-d', '{TestUtil.pgvctrl_test_db}'].\n"
            )

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.test_version}.0: {TestUtil.pgvctrl_test_repo} environment (None)\n"
        )

    def test_database_restore_db_not_empty(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.RESTORE_ARG,
                self.backup_file,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == (
                f"{TestUtil.pgvctrl_std_restore_qa_reply}"
                f"Database restores only allowed on empty databases.\n"
            )

    def test_database_restore_db_not_found(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.RESTORE_ARG,
                self.backup_file,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                "nodb",
            ])
            assert out == (
                f"{TestUtil.pgvctrl_std_restore_qa_reply}"
                f"Invalid Data Connection: ['-d', 'nodb']\n"
            )
            assert errors.code == 1

    def test_database_restore_file_not_found(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.RESTORE_ARG,
                "nofile",
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == (
                f"{TestUtil.pgvctrl_std_restore_qa_reply}"
                f"File missing: databases/_databaseBackup/pgvctrl_test/nofile\n"
            )
            assert errors.code == 1

    def test_database_restore_cancel(self):
        with mock.patch('builtins.input', return_value="NO"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.RESTORE_ARG,
                self.backup_file,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == TestUtil.pgvctrl_std_restore_cancelled_reply
            assert errors.code == 1

    def test_database_restore_cancel_any(self):
        with mock.patch('builtins.input', return_value="asdfsdfasdf"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.RESTORE_ARG,
                "nofile",
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])
            assert out == TestUtil.pgvctrl_std_restore_cancelled_reply
            assert errors.code == 1
