from dbversioning.errorUtil import VersionedDbExceptionMissingArgs
import dbversioning.dbvctrlConst as Const


def _append_svc_args(args, rtn_conn) -> bool:
    if args.svc:
        rtn_conn.append(f"service={args.svc}")
        return True
    return False


def _append_server_args(args, rtn_conn) -> bool:
    has_db = False

    if args.d:
        rtn_conn.append(Const.DATABASE_ARG)
        rtn_conn.append(args.d)
        has_db = True

    if args.host:
        rtn_conn.append(Const.PSQL_HOST_PARAM)
        rtn_conn.append(args.host)
    else:
        rtn_conn.append(Const.PSQL_HOST_PARAM)
        rtn_conn.append(Const.LOCAL_HOST)

    if args.p:
        rtn_conn.append(Const.PORT_ARG)
        rtn_conn.append(args.p)

    if args.u:
        rtn_conn.append(Const.PSQL_USER_PARAM)
        rtn_conn.append(args.u)

    return has_db


def connection_list(args):
    """
    Gets a list of connection args as needed
    :param args: 
    :return: 
    """
    rtn_conn = []
    has_svc = False
    has_db_info = False

    if args.svc:
        has_svc = _append_svc_args(args, rtn_conn)
    else:
        has_db_info = _append_server_args(args, rtn_conn)

    if (has_db_info is False) and (has_svc is False):
        raise VersionedDbExceptionMissingArgs

    return rtn_conn
