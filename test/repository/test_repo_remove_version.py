import os
import sys
from unittest import mock

from dbversioning.osUtil import dir_exists

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out)


class TestMakeVersion:
    def setup_method(self):
        TestUtil.make_conf()
        capture_dbvctrl_out(arg_list=[
            Const.MAKE_REPO_ARG,
            TestUtil.pgvctrl_test_temp_repo
        ])
        TestUtil.mkrepo_ver(
                TestUtil.pgvctrl_test_temp_repo, TestUtil.test_second_version_no_name
        )
        TestUtil.mkrepo_ver(
                TestUtil.pgvctrl_test_temp_repo, TestUtil.test_version
        )
        TestUtil.create_simple_sql_file(
                repo_name=TestUtil.pgvctrl_test_temp_repo,
                version=TestUtil.test_second_version_no_name,
                file_name="100.sql"
        )

    def teardown_method(self):
        TestUtil.remove_config()
        TestUtil.remove_root_folder()

    def test_rmv_canceled(self):
        with mock.patch('builtins.input', return_value="NO"):
            out, errors = capture_dbvctrl_out(arg_list=[
                    Const.REMOVE_V_ARG,
                    TestUtil.test_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo
            ])
            assert out == f"Do you want to remove the repository version? [YES/NO]\n" \
                          f"Repository version removal cancelled.\n"
            assert errors.code == 1

        assert dir_exists(dir_name=TestUtil.test_temp_version_no_name_path) is True
        assert dir_exists(dir_name=TestUtil.test_temp_version_path) is True

    def test_rmv(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                    Const.REMOVE_V_ARG,
                    TestUtil.test_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo
            ])


            assert out == (
                f"Do you want to remove the repository version? [YES/NO]\n"
                f"Version {TestUtil.pgvctrl_test_temp_repo}/{TestUtil.test_version} removed.\n"
            )
            assert errors is None

        assert dir_exists(dir_name=TestUtil.test_temp_version_path) is False
        assert dir_exists(dir_name=TestUtil.test_temp_version_no_name_path) is True

    def test_rmv_env(self):
        with mock.patch('builtins.input', return_value="YES"):
            capture_dbvctrl_out(arg_list=[
                Const.MAKE_ENV_ARG,
                TestUtil.env_qa,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_temp_repo,
            ])
            capture_dbvctrl_out(arg_list=[
                Const.SET_ENV_ARG,
                TestUtil.env_qa,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_temp_repo,
                Const.V_ARG,
                TestUtil.test_version,
            ])

            out, errors = capture_dbvctrl_out(arg_list=[
                    Const.REMOVE_V_ARG,
                    TestUtil.test_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo
            ])
            assert out == f"Do you want to remove the repository version? [YES/NO]\n" \
                          f"Version {TestUtil.pgvctrl_test_temp_repo}/{TestUtil.test_version} removed.\n"
            assert errors is None

        has_version_env = TestUtil.file_contains(
                TestUtil.config_file,
                '"qa": "2.0.0"',
        )

        assert dir_exists(dir_name=TestUtil.test_temp_version_path) is False
        assert dir_exists(dir_name=TestUtil.test_temp_version_no_name_path) is True
        assert has_version_env is False

    def test_rmv_bad_repo(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                    Const.REMOVE_V_ARG,
                    TestUtil.test_make_version,
                    Const.REPO_ARG,
                    TestUtil.test_first_version
            ])
            assert "Repository does not exist" in out
            assert errors.code == 1

        assert dir_exists(dir_name=TestUtil.test_temp_version_path) is True

    def test_rmv_bad_version(self):
        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                    Const.REMOVE_V_ARG,
                    TestUtil.test_make_version,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_temp_repo
            ])
            assert out == f"Do you want to remove the repository version? [YES/NO]\n" \
                   f"Repository version does not exist: {TestUtil.pgvctrl_test_temp_repo} {TestUtil.test_make_version}\n"
            assert errors.code == 1

        assert dir_exists(dir_name=TestUtil.test_temp_version_path) is True
        assert dir_exists(dir_name=TestUtil.test_temp_version_no_name_path) is True
        assert dir_exists(dir_name=f"databases/pgvctrl_temp_test/{TestUtil.test_make_version}") is False
