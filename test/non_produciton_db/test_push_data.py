import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestPushData:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.drop_database()
        TestUtil.create_database()
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_REPO_ARG,
            TestUtil.pgvctrl_test_repo
        ])
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="100.error_set.sql",
                contents="""
                            CREATE TABLE IF NOT EXISTS error_set (
                                error_id SERIAL PRIMARY KEY,
                                error_code VARCHAR NOT NULL UNIQUE,
                                error_name VARCHAR NOT NULL UNIQUE
                            );
                            INSERT INTO error_set (error_code, error_name)
                            VALUES ('1000', 'General Error'), ('2000', 'Custom General Error');
                            """
        )
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
        capture_dbvctrl_out(arg_list=[
            Const.PULL_DATA_ARG,
            Const.DATA_TBL_ARG,
            TestUtil.error_set_table_name,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()
        TestUtil.drop_database()

    def test_push_data(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f'{Const.PUSHING_DATA}\nRunning: error_set.sql\n'
        )


class TestPushDataPrePost:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.drop_database()
        TestUtil.create_database()
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_REPO_ARG,
            TestUtil.pgvctrl_test_repo
        ])
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="100.error_set.sql",
                contents="""
                                    CREATE TABLE IF NOT EXISTS error_set (
                                        error_id SERIAL PRIMARY KEY,
                                        error_code VARCHAR NOT NULL UNIQUE,
                                        error_name VARCHAR NOT NULL UNIQUE
                                    );
                                    INSERT INTO error_set (error_code, error_name)
                                    VALUES ('1000', 'General Error'), ('2000', 'Custom General Error');
                                    """
        )
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
        capture_dbvctrl_out(arg_list=[
            Const.PULL_DATA_ARG,
            Const.DATA_TBL_ARG,
            TestUtil.error_set_table_name,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
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
        TestUtil.create_simple_data_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=Const.DATA_PRE_PUSH_FILE
        )
        TestUtil.create_simple_data_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name=Const.DATA_POST_PUSH_FILE
        )

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()
        TestUtil.drop_database()

    def test_push_data(self):
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        assert out_rtn == (
            f'{Const.PUSHING_DATA}\nRunning: _pre_push.sql\n'
            f'{Const.PUSHING_DATA}\nRunning: error_set.sql\n'
            f'{Const.PUSHING_DATA}\nRunning: _post_push.sql\n'
        )
        assert errors is None


class TestPushNoDirDb:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.drop_database()
        TestUtil.create_database()
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_REPO_ARG,
            TestUtil.pgvctrl_test_repo
        ])
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()
        TestUtil.drop_database()

    def test_push_data_fail(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"No tables found to push\n"
        )


class TestPushApplyingDb:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.mkrepo(repo_name=TestUtil.pgvctrl_test_repo)
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="100.error_set.sql",
                contents="CREATE TABLE IF NOT EXISTS error_set (error_id SERIAL PRIMARY KEY);"
                         "CREATE TABLE IF NOT EXISTS app_error_set (app_error_id SERIAL PRIMARY KEY);"
        )
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.create_data_applying_config(
                repo_name=TestUtil.pgvctrl_test_repo,
                contents='[{"column-inserts": true,"table":"app_error_set","apply-order":1},{"column-inserts":true,"table":"error_set","apply-order":0}]'
        )
        TestUtil.create_simple_data_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name="error_set.sql"
        )
        TestUtil.create_simple_data_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name="app_error_set.sql"
        )

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()
        TestUtil.drop_database()

    def test_push_data(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f'{Const.PUSHING_DATA}\nRunning: error_set.sql\n'
                    f'{Const.PUSHING_DATA}\nRunning: app_error_set.sql\n'
        )

    def test_push_one_data_table(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.DATA_TBL_ARG,
                    "app_error_set",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f'{Const.PUSHING_DATA}\nRunning: app_error_set.sql\n'
        )
