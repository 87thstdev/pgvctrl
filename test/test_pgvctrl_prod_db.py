from test.test_pgvctrl import DB_REPO_CONFIG_JSON
from test.test_util import (
    TestUtil,
    print_cmd_error_details)

PROD_FLG = "-production"
VERSION = '0.0.first'
NEW_VERSION = '2.0.NewVersion'
NO_PROD_FLG = 'Production changes need the -production flag: {0}\n'
NO_PROD_FLG_APPLY = NO_PROD_FLG.format("-apply")
NO_PROD_FLG_PUSHDATA = NO_PROD_FLG.format("-pushdata")

NO_PROD_USE = "Production changes not allowed for: {0}\n"
NO_USE_APPLYFF = "Fast forwards only allowed on empty databases.\n"


class TestPgvctrlProdDb:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        pgv.run(["-init", PROD_FLG, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db], retcode=0)
        pgv.run(["-apply", PROD_FLG, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db, "-v", "0.0"],
                retcode=0)

    def teardown_method(self, test_method):
        TestUtil.drop_database()
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_chkver(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-chkver", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'{VERSION}: {TestUtil.pgvctrl_test_repo} PRODUCTION\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_version_no_prod_flag(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "0.1", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == NO_PROD_FLG_APPLY
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_version_on_production_no_prod_flag(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "0.1", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == NO_PROD_FLG_APPLY
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_version(self):
        pgv = TestUtil.local_pgvctrl()
        BAD_VER = "0.1"

        arg_list = ["-apply", "-v", BAD_VER, PROD_FLG, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Repository version does not exist: {0} {1}\n'.format(TestUtil.pgvctrl_test_repo, BAD_VER)
        assert rtn[TestUtil.return_code] == 0

    def test_apply_good_version(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-apply", "-v", "2.0", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        # TODO: Make better test
        # assert rtn[TestUtil.stdout] == '\n'.format(TestUtil.pgvctrl_test_repo)
        assert rtn[TestUtil.return_code] == 0

    def test_set_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-setff", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        # TODO: Make better test
        # assert rtn[TestUtil.stdout] == '\n'.format(TestUtil.pgvctrl_test_repo)
        assert rtn[TestUtil.return_code] == 0

    def test_apply_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-applyff", VERSION, PROD_FLG, "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]

        rtn = pgv.run(arg_list, retcode=0)
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert rtn[TestUtil.stdout] == NO_USE_APPLYFF

    def test_push_data(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-pushdata", "-repo", TestUtil.pgvctrl_test_repo, PROD_FLG, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Running: error_set.sql\n\n'
        assert rtn[TestUtil.return_code] == 0

    def test_push_data_no_prod_flag(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-pushdata", "-repo", TestUtil.pgvctrl_test_repo, "-d", TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == NO_PROD_FLG_PUSHDATA
        assert rtn[TestUtil.return_code] == 0

# TODO: Make tests for   
# -pulldata -t error_set -t membership.user_state -repo test_db -d postgresPlay
