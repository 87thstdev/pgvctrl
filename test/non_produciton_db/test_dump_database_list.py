import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)


class TestDatabaseDumpList:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.db_backups_path)
        TestUtil.delete_file(TestUtil.config_file)

    def test_dump_database_list_none(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_DD_ARG,
                ],
                msg="No database dumps available.\n"
        )

    def test_dump_database_list_simple(self):
        TestUtil.create_repo_dd_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.0"
        )
        TestUtil.create_repo_dd_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0"
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_DD_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}1.0.0                                   0.0 B\n"
                    f"{Const.TAB}2.0.0                                   0.0 B\n"
        )

    def test_dump_database_list_multi(self):
        TestUtil.create_repo_dd_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.0"
        )
        TestUtil.create_repo_dd_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0.dev"
        )

        TestUtil.create_repo_dd_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"12.0.0"
        )
        TestUtil.create_repo_dd_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"13.0.0"
        )
        TestUtil.create_repo_dd_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"13.4.0"
        )

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_DD_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_temp_repo}\n"
                    f"{Const.TAB}12.0.0                                  0.0 B\n"
                    f"{Const.TAB}13.0.0                                  0.0 B\n"
                    f"{Const.TAB}13.4.0                                  0.0 B\n"
                    f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}1.0.0                                   0.0 B\n"
                    f"{Const.TAB}2.0.0.dev                               0.0 B\n"
        )
