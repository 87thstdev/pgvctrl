from unittest import mock

import dbversioning.dbvctrlConst as Const
from test.test_util import (
    TestUtil,
    capture_dbvctrl_out,
    dbvctrl_assert_simple_msg, print_cmd_error_details)


class TestTimerSetting:
    def setup_method(self):
        TestUtil.get_static_config()

    def teardown_method(self):
        TestUtil.delete_file(TestUtil.config_file)

    def test_timer_on(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.TIMER_ON_ARG],
                msg=f"Execution Timer ON\n"
        )

    def test_timer_off(self):
        dbvctrl_assert_simple_msg(
                arg_list=[Const.TIMER_OFF_ARG],
                msg=f"Execution Timer OFF\n"
        )


class TestApplyWithTimerOn:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[Const.TIMER_ON_ARG])
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.mkrepo_ver(
            TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_first_version,
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

    def test_apply_with_timer_on(self):
        # Works in only sub minute application
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.APPLY_ARG,
                    Const.V_ARG,
                    "2.0.0",
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        output_array = out_rtn.split("\n")
        time = 0.0
        total_time = 0.0
        for ln in output_array:
            if "Time:" in ln:
                time += float(ln[10:14])

            if "Total time:" in ln:
                total_time = float(ln[16:20])

        # Because float addition
        delta = round(abs(total_time - time), 2)

        assert errors is None
        assert "Time: " in out_rtn
        assert "Total time: " in out_rtn
        assert delta <= 0.02


class TestDatabaseDumpWithTimer:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[Const.TIMER_ON_ARG])
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
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_backups_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_database_dump_with_timer(self):
        with mock.patch('builtins.input', return_value="YES"):
            out_rtn, errors = capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])

        output_array = out_rtn.split("\n")
        time = 0.00
        for ln in output_array:
            if "(time: " in ln:
                time = float(ln[50:54])

        assert errors is None
        assert "(time: " in out_rtn
        assert time > 0


class TestDatabaseRestoreWithTimer:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[Const.TIMER_ON_ARG])
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
        with mock.patch('builtins.input', return_value="YES"):
            capture_dbvctrl_out(arg_list=[
                Const.DUMP_ARG,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])

        files = TestUtil.get_backup_file_name(TestUtil.pgvctrl_test_repo)
        self.backup_file = files[0]

    def teardown_method(self):
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_backups_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_database_restore_with_timer(self):
        TestUtil.drop_database()
        TestUtil.create_database()

        with mock.patch('builtins.input', return_value="YES"):
            out, errors = capture_dbvctrl_out(arg_list=[
                Const.RESTORE_ARG,
                self.backup_file,
                Const.REPO_ARG,
                TestUtil.pgvctrl_test_repo,
                Const.DATABASE_ARG,
                TestUtil.pgvctrl_test_db,
            ])

            output_array = out.split("\n")
            count = len(
                f"Database {self.backup_file} "
                f"from repository pgvctrl_test restored ['-d', '{TestUtil.pgvctrl_test_db}']. (time: "
            )

            time = 0.00
            for ln in output_array:
                if "(time:" in ln:
                    time = float(ln[count:count+4])

            assert errors is None
            assert "(time: " in out
            assert time > 0


class TestDatabaseSetSchemaSnapshotWithTimer:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[Const.TIMER_ON_ARG])
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
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_backups_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_set_schema_snapshot_with_timer(self):
        out_rtn, errors = capture_dbvctrl_out(arg_list=[
            Const.GETSS_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

        output_array = out_rtn.split("\n")
        time = 0.00
        for ln in output_array:
            if "(time: " in ln:
                time = float(ln[79:83])

        assert errors is None
        assert "(time: " in out_rtn
        assert time > 0


class TestDatabaseApplySchemaSnapshotWithTimer:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[Const.TIMER_ON_ARG])
        capture_dbvctrl_out(arg_list=[
            Const.INIT_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])
        TestUtil.mkrepo_ver(
                TestUtil.pgvctrl_test_repo, TestUtil.test_first_version
        )
        capture_dbvctrl_out(arg_list=[
            Const.APPLY_ARG,
            Const.V_ARG,
            TestUtil.test_first_version,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ])

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_ss_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.drop_database()

    def test_apply_schema_snapshot_with_timer(self):
        capture_dbvctrl_out(
                arg_list=[
                    Const.GETSS_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        TestUtil.drop_database()
        TestUtil.create_database()

        files = TestUtil.get_snapshot_file_names(TestUtil.pgvctrl_test_repo)
        file_name = files[0]
        f_nm = file_name.rstrip('.sql')
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.APPLY_SS_ARG,
                    f_nm,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )
        output_array = out_rtn.split("\n")
        time = 0.00
        for ln in output_array:
            if f"{Const.TAB}Time: " in ln:
                time = float(ln[10:14])

        assert errors is None
        assert "Time: " in out_rtn
        assert time > 0


class TestPushApplyingWithTimerDb:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[Const.TIMER_ON_ARG])
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
        TestUtil.get_static_data_applying_config()
        TestUtil.get_static_app_error_set_data()
        TestUtil.get_static_error_set_data()

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_push_data_with_timer(self):
        out_rtn, errors = capture_dbvctrl_out(
                arg_list=[
                    Const.PUSH_DATA_ARG,
                    Const.REPO_ARG,
                    TestUtil.pgvctrl_test_repo,
                    Const.DATABASE_ARG,
                    TestUtil.pgvctrl_test_db,
                ]
        )

        output_array = out_rtn.split("\n")
        time = 0.0
        total_time = 0.0
        for ln in output_array:
            if "Time:" in ln:
                time += float(ln[10:14])

            if "Total time:" in ln:
                total_time = float(ln[12:16])

        # Because float addition
        delta = round(abs(total_time - time), 2)

        assert errors is None
        assert "Time: " in out_rtn
        assert "Total time: " in out_rtn
        assert delta <= 0.02


class TestPullDataWithTimer:
    def setup_method(self):
        TestUtil.drop_database()
        TestUtil.create_database()
        TestUtil.get_static_config()
        capture_dbvctrl_out(arg_list=[Const.TIMER_ON_ARG])
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
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)

    def teardown_method(self):
        TestUtil.delete_folder(TestUtil.test_first_version_path)
        TestUtil.delete_folder_full(TestUtil.pgvctrl_test_db_snapshots_path)
        TestUtil.delete_file(TestUtil.config_file)
        TestUtil.delete_folder_full(TestUtil.error_set_data_folder_path)
        TestUtil.drop_database()

    def test_pull_data_from_repos_table_list(self):
        arg_list = [
            Const.PULL_DATA_ARG,
            Const.DATA_TBL_ARG,
            TestUtil.error_set_table_name,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]
        # Puts tables in the list of data
        capture_dbvctrl_out(arg_list=arg_list)

        arg_list = [
            Const.PULL_DATA_ARG,
            Const.REPO_ARG,
            TestUtil.pgvctrl_test_repo,
            Const.DATABASE_ARG,
            TestUtil.pgvctrl_test_db,
        ]

        out_rtn, errors = capture_dbvctrl_out(arg_list=arg_list)

        print_cmd_error_details(out_rtn, arg_list)
        output_array = out_rtn.split("\n")
        time = 0.0

        for ln in output_array:
            if "Time:" in ln:
                time += float(ln[7:11])

        assert errors is None
        assert "Time: " in out_rtn

        assert errors is None
