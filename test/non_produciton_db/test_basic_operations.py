import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg, print_cmd_error_details)


class TestBasicDatabaseOperation:
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
        TestUtil.mkrepo_ver(
            TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_chkver_no_env(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{TestUtil.test_first_version}.0: {TestUtil.pgvctrl_test_repo} environment (None)\n"
        )

    def test_chkver_no_recs(self):
        TestUtil.remove_rev_recs(TestUtil.pgvctrl_test_db)

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"No version found!\n",
                error_code=1
        )

    def test_chkver_too_many_recs(self):
        TestUtil.add_rev_recs(TestUtil.pgvctrl_test_db)

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.CHECK_VER_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Too many version records found!\n",
                error_code=1
        )

    def test_apply_bad_version(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "0.1.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Repository version does not exist: {TestUtil.pgvctrl_test_repo} 0.1.0\n",
                error_code=1
        )

    def test_apply_good_version(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "2.0.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=TestUtil.sql_return
        )

    def test_apply_good_version_bad_sql_name(self):
        TestUtil.get_static_bad_sql_name()

        out_rtn, errors = capture_dbvctrl_out(arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "2.0.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ])

        assert errors.code == 1
        assert out_rtn.startswith("Sql filename error:")
        assert TestUtil.bad_sql_name in out_rtn

        TestUtil.delete_file(f"{TestUtil.test_sql_path}/{TestUtil.bad_sql_name}")

    def test_apply_good_version_twice(self):
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        out_rtn, errors = capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        assert errors is None
        assert f"Applied: {TestUtil.pgvctrl_test_repo} v {TestUtil.test_version}.1" in out_rtn

    def test_apply_good_version_lesser(self):
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    TestUtil.test_first_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Version is higher then applying {TestUtil.test_version} > {TestUtil.test_first_version}!\n",
                error_code=1
        )

    def test_apply_good_version_passing_v(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.ENV_ARG,
                    TestUtil.env_test,
                    Const.V_ARG,
                    "2.0.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Version and environment args are mutually exclusive.\n",
                error_code=1
        )


class TestApplyRollback:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.get_static_config()
        TestUtil.get_error_sql()
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_file(TestUtil.error_sql_path)
        TestUtil.delete_file(TestUtil.error_sql_rollback_path)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.drop_database()

    def test_apply_version_error_no_rollback(self):
        arg_list = [
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors.code == 1
        assert "AN ERROR WAS THROWN IN SQL" in out_rtn
        assert "Attempting to rollback" in out_rtn
        assert "130.Error_rollback.sql: No such file or directory" in out_rtn

    def test_apply_version_error_good_rollback(self):
        TestUtil.get_error_rollback_good_sql()
        arg_list = [
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors is None
        assert "AN ERROR WAS THROWN IN SQL" in out_rtn
        assert "Attempting to rollback" in out_rtn
        assert "130.Error_rollback" in out_rtn
        assert "140.ItemsAddMore" in out_rtn

    def test_apply_version_error_bad_rollback(self):
        TestUtil.get_error_rollback_bad_sql()
        arg_list = [
            Const.APPLY_ARG,
            Const.V_ARG,
            "2.0.0",
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

        print_cmd_error_details(out_rtn, arg_list)
        assert errors.code == 1
        assert "AN ERROR WAS THROWN IN SQL" in out_rtn
        assert "Attempting to rollback" in out_rtn
        assert "130.Error_rollback" in out_rtn
        assert "AN ERROR WAS THROWN IN THE ROLLBACK SQL" in out_rtn
        assert "140.ItemsAddMore" not in out_rtn
