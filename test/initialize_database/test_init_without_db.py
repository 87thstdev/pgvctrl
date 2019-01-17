import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)


class TestInitWithoutDb:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

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
                msg=f"Invalid Data Connection: ['{Const.DATABASE_ARG}', '{TestUtil.pgvctrl_test_db}']\n",
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

