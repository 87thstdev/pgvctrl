import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestStatusCheck:
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
        TestUtil.delete_folder_full(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_status_bad_repo(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.STATUS,
                    Const.REPO_ARG,
                    "bad",
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg="Repository does not exist: bad\n",
                error_code=1
        )

    def test_status_good_repo_missing_version_folder(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.STATUS,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Repository version does not exist: {TestUtil.pgvctrl_test_repo} {TestUtil.test_first_version}\n",
                error_code=1
        )

    def test_status_none_applied(self):
        msg = (
            f"{TestUtil.pgvctrl_test_repo}\n"
            f"{Const.TAB}v {TestUtil.test_first_version} ['test']\n"
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.STATUS,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=msg
        )

    def test_status_env(self):
        TestUtil.remove_rev_recs(TestUtil.pgvctrl_test_db)

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.STATUS,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"No version found!\n",
                error_code=1
        )


class TestStatusCheckMissingRepoFolder:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_temp_repo, TestUtil.test_first_version)
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_temp_repo_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_status_missing_repo_folder(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_temp_repo_path)

        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.STATUS,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"Missing repository folder: {TestUtil.pgvctrl_test_temp_repo}\n",
                error_code=1
        )


class TestStatusCheckComplex:
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

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_status_all_possibles(self):
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_first_version,
                file_name="40.test.sql"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_first_version,
                file_name="100.test.sql"
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_first_version,
                file_name="300.test.sql"
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

        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_first_version,
                file_name="200.test.sql"
        )
        TestUtil.append_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_first_version,
                file_name="300.test.sql",
                append="SELECT 1 as one;"
        )
        TestUtil.delete_file(f'databases/{TestUtil.pgvctrl_test_repo}/{TestUtil.test_first_version}/40.test.sql')

        msg = (
            f"{TestUtil.pgvctrl_test_repo}\n"
            f"{Const.TAB}v {TestUtil.test_first_version} ['test']\n"
            f"{Const.TABS}Missing\t\t40.test\n"
            f"{Const.TABS}Applied\t\t100.test\n"
            f"{Const.TABS}Not Applied\t200.test\n"
            f"{Const.TABS}Different\t300.test\n"
        )
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.STATUS,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=msg
        )
