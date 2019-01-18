import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out)


class TestPgvctrlTestSnapshotDb:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_snapshot_config()
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    # TODO: flesh out better snapshot functionality
    # Turn on/off per repo?
    def test_snapshot_data(self):
        files = TestUtil.get_snapshot_file_names(TestUtil.pgvctrl_test_repo)

        file_contains_insert = TestUtil.file_contains(
                f"{TestUtil.pgvctrl_test_db_snapshots_path}/{files[0]}",
                f"INSERT INTO public.repository_version (version, repository_name, is_production, env, revision, "
                f"version_hash) VALUES (NULL, 'pgvctrl_test', false, NULL, 0, NULL);"
        )

        assert files[0].split(".")[0] == "None"
        assert files[0].split(".")[2] == "sql"
        assert file_contains_insert is True
