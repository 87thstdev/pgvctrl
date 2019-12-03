import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg)


class TestMakeVersion:
    def setup_method(self):
        TestUtil.make_conf()
        TestUtil.mkrepo(repo_name=TestUtil.pgvctrl_test_repo)

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_mkv(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.MAKE_V_ARG,
                    TestUtil.test_make_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Version {TestUtil.pgvctrl_test_repo}/{TestUtil.test_make_version} created.\n"
        )

    def test_mkv_bad(self):
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.MAKE_V_ARG,
                    TestUtil.test_bad_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository version number invalid, should be [Major].[Minor].[Maintenance] "
                    f"at a minimum: {TestUtil.test_bad_version}\n",
                error_code=1
        )

    def test_mkv_exists(self):
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_V_ARG,
            TestUtil.test_make_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
        ])
        dbvctrl_assert_simple_msg(
                arg_list=[
                    Const.MAKE_V_ARG,
                    TestUtil.test_make_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo
                ],
                msg=f"Repository version already exists: {TestUtil.pgvctrl_test_repo} 3.0.0\n",
                error_code=1
        )
