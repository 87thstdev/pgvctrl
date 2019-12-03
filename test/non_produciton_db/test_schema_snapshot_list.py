import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)


class TestDatabaseSchemaSnapshotList:
    def setup_method(self):
        TestUtil.make_conf()

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

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
                file_names=["1.0.0.sql", "2.0.0.sql"]
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_SS_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}1.0.0{Const.TAB}0.0 B\n"
                    f"{Const.TAB}2.0.0{Const.TAB}0.0 B\n"
        )

    def test_schema_snapshot_list_simple_w_none_v(self):
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_names=["1.0.0.sql", "my_ss.sql"]
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_SS_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}my_ss{Const.TAB}0.0 B\n"
                    f"{Const.TAB}1.0.0{Const.TAB}0.0 B\n"

        )

    def test_schema_snapshot_list_multi(self):
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_names=["1.0.0.sql", "2.0.0.dev.sql"]
        )

        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_names=["12.0.0.sql", "13.0.0.sql", "13.4.0.sql"]
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

    def test_schema_snapshot_list_no_maintenance(self):
        TestUtil.create_repo_ss_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_names=["10.0.sql", "2.0.0.sql"]
        )

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_SS_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"{Const.TAB}2.0.0{Const.TAB}0.0 B\n"
                    f"{Const.TAB}10.0{Const.TAB}0.0 B\n"
        )
