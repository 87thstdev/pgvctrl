from errorUtil import VersionedDbExceptionMissingArgs


def _append_svc_args(args, rtn_conn):
    if args.svc:
        rtn_conn.append("service={0}".format(args.svc))


def _append_server_args(args, rtn_conn):
    if args.d:
        rtn_conn.append("-d")
        rtn_conn.append(args.d)

    if args.host:
        rtn_conn.append("-h")
        rtn_conn.append(args.host)

    if args.p:
        rtn_conn.append("-p")
        rtn_conn.append(args.p)

    if args.u:
        rtn_conn.append("-U")
        rtn_conn.append(args.u)

    if args.pwd:
        rtn_conn.append("-W")
        rtn_conn.append(args.p)


def connection_list(args):
    """
    Gets a list of connection args as needed
    :param args: 
    :return: 
    """
    rtn_conn = []

    try:
        if args.svc:
            _append_svc_args(args, rtn_conn)
        else:
            _append_server_args(args, rtn_conn)

    except TypeError:
        raise VersionedDbExceptionMissingArgs

    if len(rtn_conn) == 0:
        raise VersionedDbExceptionMissingArgs

    return rtn_conn