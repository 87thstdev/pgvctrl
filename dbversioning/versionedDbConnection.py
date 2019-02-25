from dbversioning.errorUtil import VersionedDbExceptionMissingArgs
import dbversioning.dbvctrlConst as Const


def _append_svc_args(args, rtn_conn):
    if args.svc:
        rtn_conn.append(f"service={args.svc}")


def _append_server_args(args, rtn_conn):
    if args.d:
        rtn_conn.append(Const.DATABASE_ARG)
        rtn_conn.append(args.d)

    if args.host:
        rtn_conn.append(Const.PSQL_HOST_PARAM)
        rtn_conn.append(args.host)

    if args.p:
        rtn_conn.append(Const.PORT_ARG)
        rtn_conn.append(args.p)

    if args.u:
        rtn_conn.append(Const.PSQL_USER_PARAM)
        rtn_conn.append(args.u)


def connection_list(args):
    """
    Gets a list of connection args as needed
    :param args: 
    :return: 
    """
    rtn_conn = []

    if args.svc:
        _append_svc_args(args, rtn_conn)
    else:
        _append_server_args(args, rtn_conn)

    if len(rtn_conn) == 0:
        raise VersionedDbExceptionMissingArgs

    return rtn_conn
