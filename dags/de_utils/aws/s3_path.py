from botocore.exceptions import ClientError
from pathlib import Path
from collections.abc import Iterator


def get_s3_client():
    import boto3  # delay import to make top level DAG parsing more efficient
    s3 = boto3.client("s3")
    return s3


class S3Path:
    """Class for working with objects in AWS s3.
    Intention of this class is to mimic much of the functionality available in Python pathlib
    and make working with s3 objects look and feel like a familiar file system.
    Creating buckets is not supported here as bucket naming and IAM setup should be done by admins only.
    Note we're using boto3 client instead of resource for overall faster runtime perf and greater API coverage.

    A convenience static method is included for instantiating S3Path using bucket and key separately.

    Args:
        s3_path (str): The path to s3 "folder" or "file". Format: s3://bucket/folder/filename.
            Example: s3://example-bucket/example-file.txt

    Raises:
        ValueError if s3_path arg is not formatted correctly.
    """
    def __init__(self, s3_path: str):
        self._path = s3_path
        self._bucket, self._key = self._parse_s3_path()

    @staticmethod
    def get_s3_path_from_bucket_key(bucket: str, key: str):
        """Convenience static method to return S3Path object using separate bucket and key arguments.
        Args:
            bucket (str): The name of the s3 bucket.
            key (str): The name of the s3 key.
        """
        return S3Path(f's3://{bucket}/{key}')

    def __eq__(self, other):
        """Equality is based on the s3_path given by user or created by a method like parent which captures
        the intention whether path is a dir or a file based on inclusion or exclusion of ending '/'.
        Thus, s3://bucket/key is NOT equal to s3://bucket/key/"""
        if not isinstance(other, S3Path):
            return NotImplemented
        return self._path == other._path

    def __str__(self) -> str:
        """Overriding to show path as given by user"""
        return self._path

    @property
    def bucket(self) -> str:
        """Returns the AWS S3 bucket name"""
        return self._bucket

    @property
    def key(self) -> str:
        """Returns the AWS S3 Key"""
        return self._key

    @property
    def name(self) -> str:
        """Returns a string representing the final path component"""
        path = self._path[:-1] if self._path.endswith("/") else self._path
        return path.split("/")[-1]

    @property
    def suffix(self) -> str:
        """Returns the extension if path is an object with a file type extension else return empty string ('')"""
        parts = self._path.split(".")
        if len(parts) > 1:
            return "." + parts[-1]
        return ""


    @property
    def parent(self) -> "S3Path":
        """Returns the parent of current s3_path as a new S3Path.
        Note new parent path will always include the ending file separator (/) since parent is always a dir.
        Example:  parent_s3_path = S3Path('s3://bucket/dir/file).parent
            returns the equivalent of S3Path('s3://bucket/dir/)
        """
        sep = '/'
        key = self._key[:-1] if self._key.endswith(sep) else self._key
        parts = key.split(sep)
        if len(parts) == 0 or parts == ['']:
            err = f"Cannot call parent on the path '{self}'. An s3 path must minimally contain a bucket."
            raise ValueError(err)
        new_key = sep.join(parts[:-1])
        return S3Path(f"s3://{self._bucket}/{new_key}{sep if new_key else ''}")

    def exists(self) -> bool:
        """ Returns True if the s3_path exists else False. Returns False if you do not have permissions to access the
        bucket."""
        try:
            self._head()
            return True
        except FileNotFoundError:
            return False

    def size(self) -> int:
        """Returns the size of object (file) or of all files in case of dir in bytes.
        Raises:
            FileNotFoundError if s3_path doesn't exist.
        """
        from concurrent.futures import ThreadPoolExecutor
        head = self._head()  # raises NotFoundError if not exists
        if head.get('IsFile'):
            return head['ContentLength']
        else:
            # s3_path is dir, use parallel threading to retrieve size of each object in dir tree
            with ThreadPoolExecutor(max_workers=50) as executor:
                return sum([s for s in executor.map(lambda d: d._head().get('ContentLength'), self.itertree())])

    def is_dir(self) -> bool:
        """Returns True if s3_path is an existing directory (folder) else returns False"""
        try:
            return self._head().get('IsDir', False)
        except FileNotFoundError:
            return False

    def is_file(self) -> bool:
        """Returns True if s3_path is an existing object (file) else returns False"""
        try:
            return self._head().get('IsFile', False)
        except FileNotFoundError:
            return False

    def upload_file(self, src_file: str | Path):
        """
        Uploads given local src_file to the s3_path.
        If s3_path is a dir, a file will be created in the s3 dir with the same file name as src_file.

        Raises:
            ValueError if S3Path is missing a key or if src_file is a directory.
            FileNotFoundError if src_file is not found.
            S3UploadFailedError if s3 bucket does not exist or role does not have permissions to write to the bucket.
        """
        src_file = str(src_file)
        if src_file.endswith('/'):
            raise ValueError("The src_file must be a file, not a directory.")

        key = self._key
        if key.endswith('/'):  # s3 key is a dir
            key += Path(src_file).name  # adds name of file to s3 dir

        with open(src_file, "rb") as src_fileobj:
            S3Path(f"s3://{self.bucket}/{key}").upload_fileobj(src_fileobj)

    def upload_fileobj(self, src_fileobj):
        """
        Uploads given local file type object to the s3_path.
        s3_path must be a file, not a dir.

        Raises:
            ValueError if S3Path is not a file name.
            FileNotFoundError if src_fileobj is not found.
            FileNotFoundError if s3 bucket does not exist or role does not have permissions to write to the bucket.
        """
        if not self._key:
            raise ValueError("The s3_path must be a file when uploading a file to s3.")

        key = self._key
        if key.endswith('/'):  # s3 key is a dir
            key += Path(src_fileobj).name  # adds name of file to s3 dir

        try:
            get_s3_client().upload_fileobj(src_fileobj, self._bucket, key)
        except ClientError as e:
            if "AccessDenied" in str(e):
                self._raise_file_not_found_access_denied_error()
            else:
                raise

    def upload_dir(self, src_dir: str | Path):
        """Uploads the contents of given local src_dir (recursively) to the s3_path preserving the source dir structure.

        Args:
            src_dir (string or pathlib.Path): The full path to existing local directory to upload from.

        Raises:
            FileNotFoundError if src_dir is not found.
            FileNotFoundError if s3 bucket does not exist or role does not have permissions to write to the bucket.
        """
        import s3fs  # delay import to keep Airflow top level dag loads efficient.
        try:
            s3_fs = s3fs.S3FileSystem()  # keep this out of top level code to avoid airflow/celery hanging issues.
            s3_fs.put(str(src_dir), str(self), recursive=True)
        except PermissionError:  # s3fs catches ClientError and raises generic PermissionError
            self._raise_file_not_found_access_denied_error()
        except FileNotFoundError as e:
            if "The specified bucket does not exist" in str(e):
                msg = (f"Dependency issues between s3fs and botocore/boto3 are likely causing"
                       f" FileNotFoundError-bucket does not exist which may not be true."
                       f" If bucket: {self.bucket} exists and you have permissions to it,"
                       f" then boto3/botocore versions must be adjusted to alleviate this issue.")
                raise RuntimeError(msg)
            raise  # Should be error from src_dir not existing

    def download(self, dst_path: str | Path):
        """Downloads current s3_path (dir or file) to given dst_path."""
        import s3fs  # delay import to keep Airflow top level dag loads efficient.
        try:
            s3_fs = s3fs.S3FileSystem()  # keep this out of top level code to avoid airflow/celery hanging issues.
            recursive = True if self.is_dir() else False
            s3_fs.download(str(self), str(dst_path), recursive=recursive)
        except PermissionError:  # s3fs catches ClientError and raises generic PermissionError
            self._raise_file_not_found_access_denied_error()

    def copy(self, dst_loc: str):
        """Copies current s3_path (whether file or entire directory contents) to given dst_loc (s3 or local path).
        Args:
            dst_loc (str): local or s3 destination path to copy files to.

        Examples:
            S3Path('s3://bucket/dir/').copy('/tmp/dir/')
            S3Path('s3://bucket/dir/').copy('s3:///bucket/new_dir/')
        """
        raise NotImplementedError()  # work on this if needed/desired by team

    def move(self, dst_loc: str):
        """Moves current s3_path (whether file or entire directory contents) to given destination_ (s3 or local path)"""
        raise NotImplementedError()  # work on this if needed/desired by team

    def iterdir(self) -> Iterator["S3Path"]:
        """Returns iterator for iterating over the files and dirs in immediate directory only.
        s3_path should be a directory ending with '/'.
        Returns empty iterator if no files or dirs exist in s3_path or if s3_path is an object (file).
        """
        import s3fs  # delay import to keep Airflow top level dag loads efficient.
        s3_fs = s3fs.S3FileSystem()  # keep this out of top level code to avoid airflow/celery hanging issues.
        try:
            for path in s3_fs.listdir(str(self)):
                new_path = f"s3://{path['name']}" + ("/" if path["type"] ==  "directory" else "")
                if new_path != str(self):
                    yield S3Path(new_path)
        except PermissionError:  # s3fs catches ClientError and raises generic PermissionError if bucket is not found
            self._raise_file_not_found_access_denied_error()
        except FileNotFoundError:  # if bucket is found, but dir is not found, return empty iterator
            return []

    def itertree(self, incl_dir: bool = False) -> Iterator["S3Path"]:
        """Returns an iterator to iterate over all existing S3Path objects (files) and optionally all dirs
        found in the s3_path. The current s3_path should be a directory ending with '/'.
        Returns empty iterator if no files or dirs exist in s3_path or if s3_path is an object (file).

        Args:
            incl_dir (bool): set to True to also include all subdirectories under directory. Otherwise, only objects
                (files) will be included.
        """
        kwargs = {'Bucket': self._bucket, 'Prefix': self._key}
        paths = []  # to track paths already yielded so no duplicate paths are returned.
        while True:
            try:
                resp = get_s3_client().list_objects_v2(**kwargs)
                for p in resp['Contents']:
                    path = S3Path(f"s3://{self._bucket}/{p['Key']}")
                    if path == self:
                        pass  # Don't include the current path in results
                    elif incl_dir and path.key.endswith('/'):
                        paths.append(path)  # path is a manually added folder (dir), so add it.
                        yield path
                    elif path.key.endswith('/') is False:  # path is an object (file)
                        paths.append(path)  # path is an object (file) so add it.
                        yield path
                        if incl_dir:
                            # Add any inferred "folders", those not manually created, by using parent(s) of object
                            parent = path.parent
                            while len(str(parent)) > len(str(self)):
                                if parent not in paths:
                                    paths.append(parent)
                                    yield parent
                                parent = parent.parent

                kwargs['ContinuationToken'] = \
                    resp.get('NextContinuationToken')
                if not kwargs['ContinuationToken']:
                    break
            except KeyError:
                return iter([])
            except ClientError as e:
                if 'AccessDenied' in str(e):
                    self._raise_file_not_found_access_denied_error()
                raise

    def read(self, decode: bool = True):
        """
        Returns the contents of the s3 file.

        Args:
            decode (bool): By default method will convert bytes read to string. Set to False to leave as bytes.

        Raises:
             ValueError if method is called on a bucket or dir.
             FileNotFoundError if you don't have permissions to access the s3_path or the bucket does not exist.
        """
        self._raise_if_dir(method_name="read")
        contents = get_s3_client().get_object(Bucket=self.bucket, Key=self.key)['Body'].read()
        if decode:
            contents = contents.decode("utf-8")
        return contents

    def unlink(self):
        """
        Deletes a path (a single object (file) only, not a directory) from S3.
        Buckets and directories cannot be deleted by this method.
        No error is raised if you call this method on a bucket or dir.
        Use .is_dir() method to determine if path is a dir or not.
        Use .rmtree() to delete dirs.
        Raises:
             ValueError if method is called on bucket.
             FileNotFoundError if you don't have permissions to access the s3_path or the bucket does not exist.
        """
        try:
            get_s3_client().delete_object(Bucket=self._bucket, Key=self._key)
        except ClientError as e:
            if 'AccessDenied' in str(e):
                # means either bucket doesn't exist or user doesn't have permissions to use bucket.
                self._raise_file_not_found_access_denied_error()
            raise

    def rm(self):
        """Supporting rm for user convenience. It's identical to .unlink(). See unlink method description."""
        self.unlink()

    def rmtree(self, raise_if_not_exists: bool = False):
        """
        Deletes all objects (files) recursively found in s3_path.
        Raises FileNotFound error only if raise_if_not_exists is set to True.
        """
        import s3fs  # delay import to keep Airflow top level dag loads efficient.
        try:
            s3_fs = s3fs.S3FileSystem()  # keep this out of top level code to avoid airflow/celery hanging issues.
            s3_fs.rm(str(self), recursive=True)
        except FileNotFoundError:
            if raise_if_not_exists:
                raise

    def _parse_s3_path(self) -> (str, str):
        """Returns duple with parsed bucket, key from current instance string s3_path.
        If no key is included in s3_path, returns empty string for key.
        Raises:
            ValueError if bucket is missing or s3_path doesn't start with s3://.
        """
        if not self._path.startswith('s3://'):
            raise ValueError("s3_path arg must start with 's3://' and at least include a bucket (i.e. s3://bucket)")
        path = self._path.replace('s3://', '')
        parts = path.split('/')
        bucket = parts[0]
        if len(bucket) < 2:
            raise ValueError("s3_path arg must at least include a bucket (i.e. s3://bucket)")
        key = '/'.join(parts[1:])
        return bucket, key

    def _head(self) -> dict:
        """Shared internal method returns the header metadata (as dict) of an existing S3Path (File or Dir).
        Used by various methods of S3Path to retrieve path attributes.
        Intention is to keep this call as light as possible (timely) to not slow down calling methods.
        Note adding IsDir and IsFile to result for existing dirs and files as this is quick way to check for existence.
        Raises:
            FileNotFoundError if s3_path doesn't exist. Will return successfully for a "folder" that is not manifested
                manually, but has at least 1 object in it. Thus, mimicking a standard file system.
        """
        try:
            if self._key in ['', '/']:  # path is a bucket
                head = get_s3_client().head_bucket(Bucket=self._bucket)  # raises ClientError if bucket not found
                head['IsDir'] = True  # consider existing buckets a dir
                return head
            else:  # see if path is an existing file or dir
                head = get_s3_client().head_object(Bucket=self._bucket, Key=self._key)  # raises ClientError if object not found
                if self._key.endswith('/'):  # means object is a folder manually created in s3, therefore exists
                    head['IsDir'] = True
                else:  # means it's a file that exists
                    head['IsFile'] = True
                return head
        except ClientError:
            if len(self._key) > 1:
                # check if object exists where prefix is part of this path's key. Saving this for last as it is slower.
                try:
                    if get_s3_client().list_objects_v2(
                            Bucket=self._bucket,
                            Prefix=self._key,
                            MaxKeys=1
                    )['KeyCount'] > 0:
                        # object exists so consider path an existing dir
                        return {'IsDir': True}
                except ClientError as e:
                    if 'AccessDenied' in str(e):
                        pass  # raise error below

        self._raise_file_not_found_access_denied_error()

    def _raise_file_not_found_access_denied_error(self):
        raise FileNotFoundError(f"The path '{self._path}' was not found or you do not have permission to access it.")

    def _raise_if_dir(self, method_name: str):
        """
        Raises ValueError if current path is a dir.
        Not using .is_dir .is_file methods here to keep this as fast as possible in case iterating on many files.
        """
        if self._key == '' or str(self).endswith("/"):
            raise ValueError(f"You cannot call {method_name} method on a bucket or folder.")
