"""Handles metadata cache (memory) and file cache (local endpoint)."""
import os
import shutil
import stat
import time


class MetaData(object):
    """Keep filesystem metadata in memory."""

    def __init__(self, api):
        self.api = api

        # Map file path to stat() file info dicts.
        self.files = {}

        # Map dirpath to list of files. This is technically redundant information,
        # (we could read it from self.files), but this makes listdir() faster.
        self.dirs = {}
        self.NewFile('/', stat.S_IFDIR | 0755)
        self._LoadRemoteDir('/')

    def _LoadRemoteDir(self, path):
        """Load information from a remote directory if it isn't in memory already."""
        if path in self.dirs:
            return

        data = self.api.EndpointList(path)

        # Add list of files to the directory.
        self.dirs[path] = [x['name'] for x in data]

        # Add file metadata.
        for file_info in data:
            f_type = stat.S_IFDIR if file_info['type'] == 'dir' else stat.S_IFREG
            permissions = int(file_info['permissions'], 8)  # permissions are octal
            now = time.time()
            self.files[os.path.join(path, file_info['name'])] = {
                'st_atime': now,
                'st_mtime': now,  # TODO: grab last_modified time from response
                'st_ctime': now,
                'st_nlink': 2,
                'st_mode': (f_type | permissions),
                'st_size': file_info['size']
            }

    ##############
    #    Read    #
    ##############

    def Listdir(self, path):
        """List directory contents."""
        self._LoadRemoteDir(path)
        return ['.', '..'] + self.dirs[path]

    def Stat(self, path):
        """Return stat() info for a file or None if the file doesn't exist."""
        self._LoadRemoteDir(os.path.dirname(path))
        return self.files.get(path, None)

    ##############
    #   Modify   #
    ##############

    def _AddFileToParentDir(self, path):
        if path != '/':
            self.dirs[os.path.dirname(path)].append(os.path.basename(path))

    def _RemoveFileFromParentDir(self, path):
        self.dirs[os.path.dirname(path)].remove(os.path.basename(path))

    def ChangeFileSize(self, path, size):
        self.files[path]['st_size'] = size

    def NewDirectory(self, path):
        """Create a new entry for the directory."""
        self.NewFile(path, stat.S_IFDIR | 0755)  # TODO: use given mode rather than hard-code?
        self.dirs[path] = []

    def NewFile(self, path, mode):
        """Create a new entry for the given path."""
        now = time.time()
        self.files[path] = {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                            'st_nlink': 2, 'st_mode': mode, 'st_size': 0}
        self._AddFileToParentDir(path)

    def Remove(self, path):
        """Remove file entry."""
        self.files[path] = None
        self._RemoveFileFromParentDir(path)

    def Rename(self, old_path, new_path):
        """Move a file entry to a new path."""
        self.files[new_path] = self.files[old_path]
        self._AddFileToParentDir(new_path)
        self.Remove(old_path)


class FileCache(object):
    """Handles reading/writing to the file cache."""

    def __init__(self, api, path):
        """Initialize cache under the local endpoint path."""
        self.api = api
        self.cache_dir = os.path.join(path, '.globusfs-cache')
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)
        os.mkdir(self.cache_dir)
        os.chmod(self.cache_dir, 0777)
        self.cache = {}  # Maps remote filepath to local file object.

    def Destroy(self):
        """Destroy the cache."""
        shutil.rmtree(self.cache_dir)
        print 'Deleted local cache', self.cache_dir

    def Get(self, path):
        """Get file descriptor for the given path."""
        return self.cache[path]

    def Create(self, path):
        """Create and open a new file."""
        cache_file = os.path.join(self.cache_dir, os.path.basename(path))
        self.cache[path] = open(cache_file, mode='w+')

    def Open(self, path, flags):
        """Open the given file, downloading it if necessary."""
        cache_file = os.path.join(self.cache_dir, os.path.basename(path))
        if path not in self.cache:
            if not self.api.CopyToLocal(path, cache_file):
                return None

        # Open the file and return a file handle.
        self.cache[path] = os.fdopen(os.open(cache_file, flags))
        return self.cache[path]