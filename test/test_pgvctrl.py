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


def test_mkrepo_not_exists():
    TestUtil.delete_file(DB_REPO_CONFIG_JSON)
    TestUtil.delete_folder(TestUtil.pgvctrl_no_files_repo_path)
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-mkconf"]
    pgv.run(arg_list, retcode=0)

    arg_list = ["-mkrepo", TestUtil.pgvctrl_no_files_repo]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout] == f'Repository created: {TestUtil.pgvctrl_no_files_repo}\n'
    assert rtn[TestUtil.return_code] == 0


def test_mkrepo_exists():
    TestUtil.delete_file(DB_REPO_CONFIG_JSON)
    TestUtil.delete_folder(TestUtil.pgvctrl_no_files_repo_path)
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-mkconf"]
    pgv.run(arg_list, retcode=0)

    arg_list = ["-mkrepo", TestUtil.pgvctrl_no_files_repo]
    pgv.run(arg_list, retcode=0)

    arg_list = ["-mkrepo", TestUtil.pgvctrl_no_files_repo]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout] == f'Repository already exist: {TestUtil.pgvctrl_no_files_repo}\n'
    assert rtn[TestUtil.return_code] == 0


def test_mkenv():
    TestUtil.delete_file(DB_REPO_CONFIG_JSON)
    TestUtil.delete_folder(TestUtil.pgvctrl_no_files_repo_path)
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-mkconf"]
    pgv.run(arg_list, retcode=0)

    arg_list = ["-mkrepo", TestUtil.pgvctrl_no_files_repo]
    pgv.run(arg_list, retcode=0)

    arg_list = [
        "-mkenv", TestUtil.env_test,
        "-repo", TestUtil.pgvctrl_no_files_repo
    ]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout] == "Repository environment created: {0} {1}\n".format(
            TestUtil.pgvctrl_no_files_repo,
            TestUtil.env_test
    )
    assert rtn[TestUtil.return_code] == 0


def test_repo_list():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-repolist"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0


def test_repo_list_verbose():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-repolist", "-verbose"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.return_code] == 0


def test_mkconf_not_exists():
    TestUtil.delete_file(DB_REPO_CONFIG_JSON)
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-mkconf"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout] == 'Config file created: {0}\n'.format(DB_REPO_CONFIG_JSON)
    assert rtn[TestUtil.return_code] == 0


def test_mkconf_exists():
    pgv = TestUtil.local_pgvctrl()

    arg_list = ["-mkconf"]
    pgv.run(arg_list, retcode=0)

    arg_list = ["-mkconf"]
    rtn = pgv.run(arg_list, retcode=0)

    print_cmd_error_details(rtn, arg_list)
    assert rtn[TestUtil.stdout] == 'File already exists: {0}\n'.format(DB_REPO_CONFIG_JSON)
    assert rtn[TestUtil.return_code] == 0


class TestPgvctrTestDb:
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

    def test_mkv_exists(self):
        pgv = TestUtil.local_pgvctrl()

        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        pgv.run(arg_list, retcode=0)

        arg_list = ["-mkv", TestUtil.test_make_version, "-repo", TestUtil.pgvctrl_test_repo]
        rtn = pgv.run(arg_list, retcode=0)

        print_cmd_error_details(rtn, arg_list)
        assert rtn[TestUtil.stdout] == f'Repository version already exists: {TestUtil.pgvctrl_test_repo} 3.0\n'
        assert rtn[TestUtil.return_code] == 0
