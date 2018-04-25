from test.test_util import (
    TestUtil,
    print_cmd_error_details)

DB_REPO_CONFIG_JSON = 'dbRepoConfig.json'


def test_version():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-version"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0
    assert rtn[TestUtil.stdout][:8] == 'pgvctrl:'


def test_help_h():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-h"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0


def test_help_help():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["--help"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0


class TestPgvctrRepoMakeConf:
    def setup_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_mkconf_not_exists(self):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkconf"]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'Config file created: {0}\n'.format(DB_REPO_CONFIG_JSON)
        assert rtn[TestUtil.return_code] == 0

    def test_mkconf_exists(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkconf"]
        pgv.run(arg_list, retcode=0)

        arg_list = ["-mkconf"]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == 'File already exists: {0}\n'.format(DB_REPO_CONFIG_JSON)
        assert rtn[TestUtil.return_code] == 1


class TestPgvctrRepoMakeEnv:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)

    def test_mkenv(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = [
            "-mkenv", TestUtil.env_qa,
            "-repo", TestUtil.pgvctrl_test_repo
        ]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == "Repository environment created: {0} {1}\n".format(
                TestUtil.pgvctrl_test_repo,
                TestUtil.env_qa
        )
        assert rtn[TestUtil.return_code] == 0


class TestPgvctrRepoMakeRemove:
    def setup_method(self, test_method):
        TestUtil.get_static_config()
        TestUtil.mkrepo(TestUtil.pgvctrl_no_files_repo)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.pgvctrl_no_files_repo_path)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_mkrepo_not_exists(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkrepo", TestUtil.pgvctrl_test_temp_repo]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository created: {TestUtil.pgvctrl_test_temp_repo}\n'
        assert rtn[TestUtil.return_code] == 0

    def test_mkrepo_exists(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkrepo", TestUtil.pgvctrl_no_files_repo]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository already exist: {TestUtil.pgvctrl_no_files_repo}\n'
        assert rtn[TestUtil.return_code] == 1

    def test_rmrepo_exists(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-rmrepo", TestUtil.pgvctrl_no_files_repo]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository removed: {TestUtil.pgvctrl_no_files_repo}\n'
        assert rtn[TestUtil.return_code] == 0

    def test_rmrepo_not_exists(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-rmrepo", TestUtil.pgvctrl_test_temp_repo]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository does not exist: {TestUtil.pgvctrl_test_temp_repo}\n'
        assert rtn[TestUtil.return_code] == 1


class TestPgvctrRepoList:
    def setup_method(self, test_method):
        TestUtil.get_static_config()
        TestUtil.mkrepo_ver(TestUtil.pgvctrl_test_repo, TestUtil.test_first_version)

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.test_make_version_path)
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder(TestUtil.pgvctrl_test_temp_repo_path)

    def test_repo_list(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-rl"]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert rtn[TestUtil.stdout] == f'{TestUtil.pgvctrl_test_repo}\n' \
                                       f'\tv {TestUtil.test_first_version} test\n' \
                                       f'\tv {TestUtil.test_version} \n'

    def test_repo_list_verbose(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-rlv"]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert rtn[TestUtil.stdout] == f'{TestUtil.pgvctrl_test_repo}\n' \
                                       f'\tv {TestUtil.test_first_version} test\n' \
                                       f'\tv {TestUtil.test_version} \n' \
                                       f'\t\t100 AddUsersTable\n' \
                                       f'\t\t105 Notice\n' \
                                       f'\t\t110 Error\n' \
                                       f'\t\t200 AddEmailTable\n' \
                                       f'\t\t300 UserStateTable\n' \
                                       f'\t\t400 ErrorSet\n' \


    def test_repo_list_unregistered(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkrepo", TestUtil.pgvctrl_test_temp_repo]
        pgv.run(arg_list, retcode=0)

        arg_list = ["-rmrepo", TestUtil.pgvctrl_test_temp_repo]
        pgv.run(arg_list, retcode=0)

        arg_list = ["-rl"]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.return_code] == 0
        assert rtn[TestUtil.stdout] == f'{TestUtil.pgvctrl_test_temp_repo} UNREGISTERED\n' \
                                       f'{TestUtil.pgvctrl_test_repo}\n' \
                                       f'\tv {TestUtil.test_first_version} test\n' \
                                       f'\tv {TestUtil.test_version} \n'


class TestPgvctrMakeVersion:
    def setup_method(self, test_method):
        TestUtil.get_static_config()

    def teardown_method(self, test_method):
        TestUtil.delete_file(DB_REPO_CONFIG_JSON)
        TestUtil.delete_folder(TestUtil.test_make_version_path)

    def test_mkv(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Version {TestUtil.pgvctrl_test_repo}/{TestUtil.test_make_version} created.\n'
        assert rtn[TestUtil.return_code] == 0

    def test_mkv_bad(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkv", TestUtil.test_bad_version, "-repo", TestUtil.pgvctrl_test_repo]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository version number invalid, should be [Major].[Minor].[Maintenance] ' \
                                       f'at a minimum: {TestUtil.test_bad_version}\n'
        assert rtn[TestUtil.return_code] == 1

    def test_mkv_exists(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        pgv.run(arg_list, retcode=0)

        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        rtn = pgv.run(arg_list, retcode=1)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository version already exists: {TestUtil.pgvctrl_test_repo} 3.0.0\n'
        assert rtn[TestUtil.return_code] == 1
