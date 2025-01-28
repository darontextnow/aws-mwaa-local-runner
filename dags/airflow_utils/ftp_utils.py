"""Module contains utils for use by Airflow related to FTP/FTPS"""
from datetime import datetime


def get_max_modified_file_from_ftp_dir(ftp_connection_id: str, ftp_dir: str) -> (str, datetime):
    """Returns (filename, modified timestamp in pytz.UTC) of file in given directory with max modified time."""
    from airflow.providers.ftp.hooks.ftp import FTPSHook
    import pytz

    ftp_hook = FTPSHook(ftp_conn_id=ftp_connection_id)
    last_fname = None
    last_mod = None
    for fname in ftp_hook.list_directory(ftp_dir):
        mod = ftp_hook.get_mod_time(f"{ftp_dir}/{fname}").astimezone(pytz.utc)
        if last_mod is None or mod > last_mod:
            last_fname = fname
            last_mod = mod
    return last_fname, last_mod
