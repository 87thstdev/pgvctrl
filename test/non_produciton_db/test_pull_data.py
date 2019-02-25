import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    print_cmd_error_details,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestPullData:
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
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_pull_data_no_list(self):
        arg_list = [
            Const.PULL_DATA_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]

        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg="No tables to pull!\n",
                error_code=1
        )

    def test_pull_data_bad_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PULL_DATA_ARG,
                    Const.DATA_TBL_ARG,
                    TestUtil.bad_table_name,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Pulling: {TestUtil.bad_table_name}\nSql Error: pg_dump: no matching tables were found\n\n",
                error_code=1
        )

        has_table = TestUtil.file_contains(TestUtil.test_version_data_path, TestUtil.bad_table_name)
        assert not has_table

    def test_pull_data_from_repos_table_list(self):
        arg_list = [
            Const.PULL_DATA_ARG,
            Const.DATA_TBL_ARG,
            TestUtil.error_set_table_name,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        # Puts tables in the list of data
        capture_dbvctrl_out(arg_list=arg_list)

        arg_list = [
            Const.PULL_DATA_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]

        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)
        has_error_set = TestUtil.file_contains(TestUtil.error_set_data_path, TestUtil.error_set_table_name)
        has_custom_error = TestUtil.file_contains(TestUtil.error_set_data_path, TestUtil.custom_error_message)
        has_error_set_default = TestUtil.file_contains(TestUtil.test_version_data_path, TestUtil.error_set_table_error_set_table)

        print_cmd_error_details(out_rtn, arg_list)
        assert "Pulling: error_set" in out_rtn
        assert has_error_set
        assert has_custom_error
        assert has_error_set_default
        assert errors is None

