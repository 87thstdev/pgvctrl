import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)

NO_PROD_FLG_PUSHDATA = f"Production changes need the -production flag: {Const.PUSH_DATA_ARG}\n"


class TestProdPushingDatabase:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.mkrepo(repo_name=TestUtil.pgvctrl_test_repo)
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                version=TestUtil.test_version,
                file_name="100.error_set.sql",
                contents="CREATE TABLE IF NOT EXISTS error_set (error_id SERIAL PRIMARY KEY);"
        )
        capture_dbvctrl_out(arg_list=[
                    Const.INIT_ARG,
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ])
        TestUtil.create_data_applying_config(
                repo_name=TestUtil.pgvctrl_test_repo,
                contents='[{"column-inserts":true,"table":"error_set","apply-order":0}]'
        )
        TestUtil.create_simple_data_sql_file(
                repo_name=TestUtil.pgvctrl_test_repo,
                file_name="error_set.sql"
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
                    Const.PRODUCTION_ARG,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )
        assert out_rtn == f"{Const.PUSHING_DATA}\nRunning: error_set.sql\n"
        assert errors is None

    def test_push_data_no_prod_flag(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=NO_PROD_FLG_PUSHDATA,
                error_code=1
        )
