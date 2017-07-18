from errorUtil import VersionedDbExceptionMissingArgs


def connection_list(args):
    """
    Gets a list of connection args as needed
    :param args: 
    :return: 
    """
    rtn_conn = []

    try:
        if args.d:
            rtn_conn.append("-d")
            rtn_conn.append(args.d)

    except TypeError:
        raise VersionedDbExceptionMissingArgs

    if len(rtn_conn) == 0:
        raise VersionedDbExceptionMissingArgs

    return rtn_conn
