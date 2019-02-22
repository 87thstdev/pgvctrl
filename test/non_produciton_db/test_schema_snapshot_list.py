import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)


class TestDatabaseSchemaSnapshotList:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_databases_ss_path)
        TestUtil.delete_file(TestUtil.config_file)

    def test_schema_snapshot_list_none(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_SS_ARG,
                ],
                msg="No Schema Snapshots available.\n"
        )

    def test_schema_snapshot_list_simple(self):
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.0.sql"
        )
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0.sql"
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_SS_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}1.0.0{Const.TAB}0.0 B\n"
                    f"{Const.TAB}2.0.0{Const.TAB}0.0 B\n"
        )

    def test_schema_snapshot_list_multi(self):
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.0.sql"
        )
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0.dev.sql"
        )

        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"12.0.0.sql"
        )
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"13.0.0.sql"
        )
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"13.4.0.sql"
        )

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_SS_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_temp_repo}\n"
                    f"{Const.TAB}12.0.0{Const.TAB}0.0 B\n"
                    f"{Const.TAB}13.0.0{Const.TAB}0.0 B\n"
                    f"{Const.TAB}13.4.0{Const.TAB}0.0 B\n"
                    f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}1.0.0{Const.TAB}0.0 B\n"
                    f"{Const.TAB}2.0.0.dev{Const.TAB}0.0 B\n"
        )

    def test_schema_snapshot_list_fail(self):
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.sql"
        )
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0.sql"
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_SS_ARG,
                ],
                msg=f"pgvctrl_test\n"
                    f"Repository version number invalid, should be [Major].[Minor].[Maintenance] "
                    f"at a minimum: 1.0.sql\n",
                error_code=1
        )
