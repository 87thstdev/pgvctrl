from test.test_util import (
    TestUtil,
    print_cmd_error_details)


class TestPgvctrInitlWithOutDb:
    def setup_method(self, test_method):
        TestUtil.drop_database()

    def test_init(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-init", "-repo", TestUtil.pgvctrl_test_temp_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == "Invalid Data Connection: ['-d', '{0}']\n".format(TestUtil.pgvctrl_test_db)
        assert rtn[TestUtil.return_code] == 0


class TestPgvctrInitlWithDb:
    def setup_method(self, test_method):
        TestUtil.create_database()

    def teardown_method(self, test_method):
        TestUtil.drop_database()

    def test_init(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-init", "-repo", TestUtil.pgvctrl_test_temp_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Database initialized\n'
        assert rtn[TestUtil.return_code] == 0

    def test_init_production(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-init", "-repo", TestUtil.pgvctrl_test_temp_repo, "-production",  "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)
        
        print_cmd_error_details(rtn, arg_list)        
        assert rtn[TestUtil.stdout] == 'Database initialized (PRODUCTION)\n'
        assert rtn[TestUtil.return_code] == 0
