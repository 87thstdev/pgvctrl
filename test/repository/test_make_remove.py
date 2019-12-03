import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg)


class TestRepoMakeRemove:
    def setup_method(self):
        TestUtil.make_conf()

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_mkrepo_not_exists(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo],
                msg=f"Repository created: {TestUtil.pgvctrl_test_temp_repo}\n"
        )

    def test_mkrepo_exists(self):
        TestUtil.mkrepo(TestUtil.pgvctrl_no_files_repo)

        dbvctrl_assert_simple_msg(
                arg_list=[Const.MAKE_REPO_ARG, TestUtil.pgvctrl_no_files_repo],
                msg=f"Repository already exist: {TestUtil.pgvctrl_no_files_repo}\n",
                error_code=1
        )

    def test_mkrepo_bad_name(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.MAKE_REPO_ARG, TestUtil.invalid_repo_name],
                msg=f"Repository name invalid {TestUtil.invalid_repo_name}\n",
                error_code=1
        )

    def test_rmrepo_exists(self):
        TestUtil.mkrepo(TestUtil.pgvctrl_no_files_repo)

        dbvctrl_assert_simple_msg(
                arg_list=[Const.REMOVE_REPO_ARG, TestUtil.pgvctrl_no_files_repo],
                msg=f"Repository removed: {TestUtil.pgvctrl_no_files_repo}\n"
        )

    def test_rmrepo_not_exists(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.REMOVE_REPO_ARG, TestUtil.pgvctrl_test_temp_repo],
                msg=f"Repository does not exist: {TestUtil.pgvctrl_test_temp_repo}\n",
                error_code=1
        )
