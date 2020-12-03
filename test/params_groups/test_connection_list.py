from dbversioning.dbvctrl import parse_args
from dbversioning.versionedDbConnection import connection_list
import dbversioning.dbvctrlConst as Const


def test_connection_list_min():
    args = parse_args(args=[
        Const.DATABASE_ARG, "test"
    ])
    t = connection_list(args=args)
    assert t == ['-d', 'test', '-h', 'localhost']


def test_connection_list_on_server_db():
    args = parse_args(args=[
        Const.ON_SERVER,
        Const.DATABASE_ARG, "test"
    ])
    t = connection_list(args=args)
    assert t == ['-d', 'test']


def test_connection_list_on_server():
    args = parse_args(args=[
        Const.ON_SERVER
    ])
    t = connection_list(args=args)
    assert t == []
