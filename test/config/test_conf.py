import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    dbvctrl_assert_simple_msg,
    capture_dbvctrl_out)


class TestBadConf:
    def setup_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_mkconf_not_exists(self):
        TestUtil.delete_file(TestUtil.config_file)

        arg_list = [Const.MKCONF_ARG]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"Config file created: {TestUtil.config_file}\n"
        )

    def test_mkconf_exists(self):
        arg_list = [Const.MKCONF_ARG]

        # Make config first
        capture_dbvctrl_out(arg_list=arg_list)

        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"File already exists: {TestUtil.config_file}\n",
                error_code=1
        )

    def test_conf_bad_root(self):
        TestUtil.get_static_bad_config()

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"Invalid key in config, expected 'root'\n",
                error_code=1
        )

    def test_conf_bad_repositories(self):
        TestUtil.get_static_bad_repositories_config()

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"Invalid key in config, expected 'repositories'\n",
                error_code=1
        )

    def test_conf_bad_multi_repositories(self):
        TestUtil.get_static_bad_config_multi_repos()

        dbvctrl_assert_simple_msg(
                arg_list=[Const.LIST_REPOS_ARG],
                msg=f"Bad config: Multiple repositories found for {TestUtil.pgvctrl_test_repo}!\n",
                error_code=1
        )

    def test_conf_no_mkconf(self):
        arg_list = [Const.LIST_REPOS_ARG]
        dbvctrl_assert_simple_msg(
                arg_list=arg_list,
                msg=f"File missing: {TestUtil.config_file}\n",
                error_code=1
        )
