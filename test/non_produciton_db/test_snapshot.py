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
        assert True == True
