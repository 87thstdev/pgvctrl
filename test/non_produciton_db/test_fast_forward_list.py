import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)


class TestDatabaseFastForwardList:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_databases_ff_path)
        TestUtil.delete_file(TestUtil.config_file)

    def test_fast_forward_list_none(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_REPOS_FF_ARG,
                ],
                msg="No fast forwards available.\n"
        )

    def test_fast_forward_list_simple(self):
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.0.sql"
        )
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0.sql"
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_REPOS_FF_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_repo}\n"
                    f"\t1.0.0\t0.0 B\n"
                    f"\t2.0.0\t0.0 B\n"
        )

    def test_fast_forward_list_multi(self):
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.0.sql"
        )
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0.dev.sql"
        )

        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"12.0.0.sql"
        )
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"13.0.0.sql"
        )
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                file_name=f"13.4.0.sql"
        )

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_REPOS_FF_ARG,
                ],
                msg=f"{TestUtil.pgvctrl_test_temp_repo}\n"
                    f"\t12.0.0\t0.0 B\n"
                    f"\t13.0.0\t0.0 B\n"
                    f"\t13.4.0\t0.0 B\n"
                    f"{TestUtil.pgvctrl_test_repo}\n"
                    f"\t1.0.0\t0.0 B\n"
                    f"\t2.0.0.dev\t0.0 B\n"
        )

    def test_fast_forward_list_fail(self):
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"1.0.sql"
        )
        TestUtil.create_repo_ff_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=f"2.0.0.sql"
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.LIST_REPOS_FF_ARG,
                ],
                msg=f"pgvctrl_test\n"
                    f"Repository version number invalid, should be [Major].[Minor].[Maintenance] "
                    f"at a minimum: 1.0.sql\n",
                error_code=1
        )
