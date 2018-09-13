import dbversioning.dbvctrlConst as Const
from test.test_pgvctrl_config import DB_REPO_CONFIG_JSON
from test.test_util import (
    TestUtil,
    print_cmd_error_details)

NO_PROD_FLG = 'Production changes need the -production flag: {0}\n'
NO_PROD_FLG_APPLY = NO_PROD_FLG.format(Const.APPLY_ARG)
NO_PROD_FLG_PUSHDATA = NO_PROD_FLG.format(Const.PULL_DATA_ARG)

NO_PROD_USE = "Production changes not allowed for: {0}\n"
NO_USE_APPLYFF = "Fast forwards only allowed on empty databases.\n"
BAD_VER = "0.1.0"


class TestPgvctrlProdDb:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        pgv.run([Const.INIT_ARG, Const.PRODUCTION_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG,
                 TestUtil.pgvctrl_test_db], retcode=0)
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)
        pgv.run([Const.APPLY_ARG, Const.PRODUCTION_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG,
                 TestUtil.pgvctrl_test_db, Const.V_ARG,
                 "0.0.0"],
                retcode=0)

    def teardown_method(self, test_method):
        TestUtil.drop_database()
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_chkver_no_env(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.CHECK_VER_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'{TestUtil.test_first_version}.0: ' \
                                       f'{TestUtil.pgvctrl_test_repo} PRODUCTION environment (None)\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_bad_version_no_prod_flag(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.APPLY_ARG, Const.V_ARG, BAD_VER, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == NO_PROD_FLG_APPLY
        assert rtn[TestUtil.return_code] == 1

    def test_apply_bad_version_on_production_no_prod_flag(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.APPLY_ARG, Const.V_ARG, BAD_VER, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == NO_PROD_FLG_APPLY
        assert rtn[TestUtil.return_code] == 1

    def test_apply_bad_version(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.APPLY_ARG, Const.V_ARG, BAD_VER, Const.PRODUCTION_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository version does not exist: {TestUtil.pgvctrl_test_repo} {BAD_VER}\n'
        assert rtn[TestUtil.return_code] == 1

    def test_apply_good_version(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.APPLY_ARG, Const.V_ARG, "2.0.0", Const.PRODUCTION_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == TestUtil.sql_return
        assert rtn[TestUtil.return_code] == 0

    def test_set_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.SETFF_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Fast forward set: {TestUtil.pgvctrl_test_repo}\n'
        assert rtn[TestUtil.return_code] == 0

    def test_apply_fast_forward(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.APPLY_FF_ARG, TestUtil.test_first_version, Const.PRODUCTION_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db]

        rtn = pgv.run(arg_list, retcode=1)
        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 1
        assert rtn[TestUtil.stdout] == NO_USE_APPLYFF

    def test_push_data(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.PUSH_DATA_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.PRODUCTION_ARG,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Running: error_set.sql\n\n'
        assert rtn[TestUtil.return_code] == 0

    def test_push_data_no_prod_flag(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.PUSH_DATA_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == NO_PROD_FLG_PUSHDATA
        assert rtn[TestUtil.return_code] == 1


class TestPgvctrlProdDbEnv:
    def setup_method(self, test_method):
        pgv = TestUtil.local_pgvctrl()
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        pgv.run([Const.INIT_ARG, Const.PRODUCTION_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG,
                 TestUtil.pgvctrl_test_db, Const.SET_ENV_ARG,
                 TestUtil.env_test], retcode=0)
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)
        pgv.run([Const.APPLY_ARG, Const.PRODUCTION_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG,
                 TestUtil.pgvctrl_test_db, Const.ENV_ARG,
                 TestUtil.env_test],
                retcode=0)

    def teardown_method(self, test_method):
        TestUtil.drop_database()
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_chkver_env(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [Const.CHECK_VER_ARG, Const.REPO_ARG, TestUtil.pgvctrl_test_repo, Const.DATABASE_ARG, TestUtil.pgvctrl_test_db]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'{TestUtil.test_first_version}.0: ' \
                                       f'{TestUtil.pgvctrl_test_repo} PRODUCTION environment (' \
                                       f'{TestUtil.env_test})\n'
        assert rtn[TestUtil.return_code] == 0

# TODO: Make tests for
# -pulldata -t error_set -t membership.user_state -repo test_db -d postgresPlay
