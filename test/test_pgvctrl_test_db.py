from test.test_util import (
    TestUtil,
    print_cmd_error_details)


class TestPgvctrTestDb:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.create_database()
        rtn = pgv.run(["-init", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db], retcode=0)

    def teardown_method(self, test_method):
        TestUtil.drop_database()

    def test_chkver(self):
        pgv = TestUtil.local_pgvctrl()
    
        arg_list = ["-chkver", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == '0.0.gettingStarted: {0}\n'.format(TestUtil.pgvctrl_test_repo)
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_version(self):
        pgv = TestUtil.local_pgvctrl()
    
        arg_list = ["-apply", "-v", "0.1", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Repository version does not exist: {0} 0.1\n'.format(TestUtil.pgvctrl_test_repo)
        assert rtn[TestUtil.return_code] == 0
    
    def test_apply_good_version(self):
        pgv = TestUtil.local_pgvctrl()
    
        arg_list = ["-apply", "-v", "2.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        # TODO: Make better test
        # assert rtn[TestUtil.stdout] == '\n'.format(TestUtil.pgvctrl_test_repo)
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
    
    def test_set_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()
    
        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)
        

        # TODO: Make better test
        # assert rtn[TestUtil.stdout] == '\n'.format(TestUtil.pgvctrl_test_repo)        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0

    def test_apply_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()
        good_ff = "0.0.gettingStarted"

        arg_list = ["-applyff", good_ff, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0

    def test_push_data(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-pushdata", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Running: error_set.sql\n\n'
        assert rtn[TestUtil.return_code] == 0

    def test_mkv(self):
        TestUtil.delete_folder(TestUtil.test_version_path)
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkv", TestUtil.test_version, "-repo", TestUtil.pgvctrl_test_temp_repo]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == "Version {0}/{1} created.\n".format(
            TestUtil.pgvctrl_test_temp_repo,
            TestUtil.test_version
        )
        assert rtn[TestUtil.return_code] == 0

    def test_mkv_exists(self):
        TestUtil.delete_folder(TestUtil.test_version_path)
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkv", TestUtil.test_version, "-repo", TestUtil.pgvctrl_test_temp_repo]
        pgv.run(arg_list, retcode=0)
        
        arg_list = ["-mkv", TestUtil.test_version, "-repo", TestUtil.pgvctrl_test_temp_repo]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == "Repository version already exists: {0} 2.0\n".format(
            TestUtil.pgvctrl_test_temp_repo
        )
        assert rtn[TestUtil.return_code] == 0


# TODO: Make tests for
# -pulldata -t error_set -t membership.user_state -repo test_db -d postgresPlay


class TestPgvctrTestCleanDb:
    def setup_method(self):
        TestUtil.create_database()
        
    def teardown_method(self, test_method):
        TestUtil.drop_database()

    def test_apply_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()
        good_ff = "0.0.gettingStarted"

        arg_list = ["-applyff", good_ff,  "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Applying: {0}\n\n'.format(good_ff)
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
        good_ff = "0.0.gettingStarted"
        bad_db = "asdfasdfasdfasdfa123123asdfa"

        arg_list = ["-applyff", good_ff,  "-repo", TestUtil.pgvctrl_test_repo, "-d", bad_db]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == "Invalid Data Connection: ['-d', '{0}']\n".format(bad_db)
        assert rtn[TestUtil.return_code] == 0
