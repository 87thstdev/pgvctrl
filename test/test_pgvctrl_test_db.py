from test.test_pgvctrl import DB_REPO_CONFIG_JSON
from test.test_util import (
    TestUtil,
    print_cmd_error_details)


class TestPgvctrTestDb:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.create_database()
        TestUtil.get_static_config()
        pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db], retcode=0)
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)
        pgv.run(["-apply", "-v", TestUtil.test_first_version, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db],
                retcode=0)

    def teardown_method(self, test_method):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.drop_database()

    def test_chkver_no_env(self):
        pgv = TestUtil.local_pgvctrl()
    
        arg_list = ["-chkver", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'{TestUtil.test_first_version}.0: {TestUtil.pgvctrl_test_repo} environment (' \
                                       f'None)\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_version(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "0.1.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Repository version does not exist: {0} 0.1.0\n'.format(
                TestUtil.pgvctrl_test_repo)
        assert rtn[TestUtil.return_code] == 0

    def test_apply_good_version(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "2.0.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert rtn[TestUtil.stdout] == TestUtil.sql_return

    def test_set_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-applyff", TestUtil.test_first_version, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Fast forwards only allowed on empty databases.\n'
        assert rtn[TestUtil.return_code] == 0

    def test_push_data(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-pushdata", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Running: error_set.sql\n\n'
        assert rtn[TestUtil.return_code] == 0


# TODO: Make tests for
# -pulldata -t error_set -t membership.user_state -repo test_db -d postgresPlay


class TestPgvctrTestCleanDb:
    def setup_method(self):
        TestUtil.create_database()
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.drop_database()

    def test_apply_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-applyff", TestUtil.test_first_version,  "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Applying: {TestUtil.test_first_version}\n\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()
        bad_ff = "BAD"

        arg_list = ["-applyff", bad_ff,  "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Fast forward not found {0}\n'.format(bad_ff)
        assert rtn[TestUtil.return_code] == 0

    def test_apply_fast_forward_bad_db(self):
        pgv = TestUtil.local_pgvctrl()
        good_ff = "0.0.0.gettingStarted"
        bad_db = "asdfasdfasdfasdfa123123asdfa"

        arg_list = ["-applyff", good_ff,  "-repo", TestUtil.pgvctrl_test_repo, "-d", bad_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == "Invalid Data Connection: ['-d', '{0}']\n".format(bad_db)
        assert rtn[TestUtil.return_code] == 0


class TestPgvctrTestDbEnv:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.create_database()
        TestUtil.get_static_config()
        pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db, "-setenv",
                 TestUtil.env_test], retcode=0)
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)
        pgv.run(["-apply", "-env", TestUtil.env_test, "-repo", TestUtil.pgvctrl_test_repo, "-d",
                 TestUtil.pgvctrl_test_db], retcode=0)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.drop_database()

    def test_chkver_no_env(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-chkver", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[
                   TestUtil.stdout] == f'{TestUtil.test_first_version}.0: {TestUtil.pgvctrl_test_repo} environment (' \
                                       f'{TestUtil.env_test})\n'
        assert rtn[TestUtil.return_code] == 0
