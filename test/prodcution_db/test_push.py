import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)

NO_PROD_FLG_PUSHDATA = f"Production changes need the -production flag: {Const.PUSH_DATA_ARG}\n"


class TestProdPushingDatabase:
    def setup_method(self):
        TestUtil.get_static_data_config()
        TestUtil.get_static_error_set_data()
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[
                    Const.INIT_ARG,
                    Const.PRODUCTION_ARG,
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
                    Const.PRODUCTION_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                    Const.V_ARG,
                    "0.0.0",
            ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)

    def test_push_data(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.PRODUCTION_ARG,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"{Const.PUSHING_DATA}\nRunning: error_set.sql\n"
        )

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
