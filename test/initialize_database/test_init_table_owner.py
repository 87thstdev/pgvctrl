import dbversioning.dbvctrlConst as Const
from dbversioning.repositoryconf import REPOSITORY_VERSION
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg,
    capture_dbvctrl_out)


class TestInitWithTblOwner:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.remove_table_owner_role()
        TestUtil.create_table_owner_role()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)
        capture_dbvctrl_out(arg_list=[
            Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
            TestUtil.version_table_owner,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
        ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.remove_table_owner_role()
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_init(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg="Database initialized\n"
        )
        owner = TestUtil.get_table_owner(
                db_name=TestUtil.pgvctrl_test_db,
                table_name=REPOSITORY_VERSION
        )
        assert owner == TestUtil.version_table_owner


class TestInitWithTblOwnerWithDbRoleMissing:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.remove_table_owner_role()
        TestUtil.create_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_test_temp_repo)
        capture_dbvctrl_out(arg_list=[
            Const.SET_VERSION_STORAGE_TABLE_OWNER_ARG,
            TestUtil.version_table_owner,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo,
        ])

    def teardown_method(self):
        TestUtil.drop_database()
        TestUtil.remove_table_owner_role()
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_init_fail(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.INIT_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ],
                msg=f"DB Error ERROR:  role \"{TestUtil.version_table_owner}\" does not exist\n\n",
                error_code=1
        )
