from pathlib import Path


def posix_path_itertree(posix_dir: str | Path, glob_pattern: str = "**/*", incl_dirs: bool = True):
    """Returns Iterator[PosixPath] for recursively iterating through all paths in given posix_dir.

    Args:
        posix_dir (string): Must be an existing directory.
        glob_pattern (string): Default pattern ('**') includes all subdirectories and files under given posix_dir.
            Any glob_pattern supplied here will be appended to the given posix_dir.
        incl_dirs (bool): Set to False to only return files and not include directories in output.

    Raises:
        ValueError if given posix_dir is not an existing dir.
    """
    path = Path(posix_dir)
    if path.is_dir() is False:
        raise ValueError(f"The posix_dir '{posix_dir}' is not an existing directory.")

    for filename in path.rglob(glob_pattern):
        if filename == path:
            continue  # don't include the posix_dir itself in the output
        is_dir = filename.is_dir()
        if incl_dirs is True and is_dir:
            filename.joinpath("/")  # add end forward slash to dir
        if incl_dirs is False and is_dir:
            continue  # not including dirs
        yield filename
