import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    LOCAL_HOST,
    dbvctrl_assert_simple_msg)


class TestInitWithoutDb:
    def setup_method(self):
        TestUtil.create_config()
        TestUtil.drop_database()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_init_invalid(self):
        arg_list = [
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"Invalid Data Connection: ['{Const.DATABASE_ARG}', '{TestUtil.pgvctrl_test_db}', "
                    f"'{Const.PSQL_HOST_PARAM}', '{LOCAL_HOST}']\n",
                error_code=1
        )

    def test_init_invalid_db_conn(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                ],
                msg=f"Missing connection args\n",
                error_code=1
        )

