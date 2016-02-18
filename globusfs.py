#!/usr/bin/env python
# Copyright (c) 2016 Austin Byers <austin.b.byers@gmail.com>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
"""Mount a Globus endpoint with FUSE."""

import argparse
import errno
import os
import stat
import time

from fuse import FUSE, FuseOSError, Operations
from globusonline.transfer import api_client


class GlobusFS(Operations):

    # TODO: encryption
    # TODO: tests
    # TODO: queue requests into batches (e.g. rm -r should not send so many reqs)
    # TODO: we probably must update cache every now and then to pull updates

    def __init__(self, endpoint):
        # Get credentials and activate endpoint.
        auth_result = api_client.goauth.get_access_token()
        self.api = api_client.TransferAPIClient(
            username=auth_result.username, goauth=auth_result.token)
        self.endpoint = endpoint
        _, _, data = self.api.endpoint_autoactivate(endpoint)
        print data['message']

        # Cache file metadata in memory.
        now = time.time()
        root_stat = {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                     'st_nlink': 2, 'st_mode': (stat.S_IFDIR | 0755)}
        self.files = {'/': root_stat}  # Map filepath to stat() file info dictionaries.
        self.dirs = {}  # Map dirname to list of filenames.

    def _LoadDir(self, path):
        """Load directory information from endpoint if it isn't in memory already."""
        if path in self.dirs:
            return

        print '\t--> Loading directory %s from Globus' % path
        _, _, data = self.api.endpoint_ls(self.endpoint, path=path)

        # Add list of file names to directory (for readdir())
        self.dirs[path] = [x['name'] for x in data['DATA']]

        # Add file metadata (for getattr())
        for file_info in data['DATA']:
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

    def _TaskID(self):
        """Get a new task id."""
        _, _, result = self.api.transfer_submission_id()
        return result['value']

    def _Copy(self, src_path, dest_path):
        """Submit a task to recursively copy to and from the same endpoint."""
        # TODO: should be recursive only if directory
        rename_task = api_client.Transfer(self._TaskID(), self.endpoint, self.endpoint)
        rename_task.add_item(src_path, dest_path, recursive=True)
        _, _, data = self.api.transfer(rename_task)
        print data['message']

    def _Delete(self, path):
        """Submit a task to recursively delete the given path."""
        delete_task = api_client.Delete(self._TaskID(), self.endpoint, recursive=True)
        delete_task.add_item(path)
        _, _, data = self.api.delete(delete_task)
        print data['message']

    def getattr(self, path, fh=None):
        """Get metadata for a specific file/directory."""
        # Load the parent directory if we haven't already.
        self._LoadDir(os.path.dirname(path))
        if path not in self.files:
            # File doesn't exist.
            raise FuseOSError(errno.ENOENT)
        return self.files[path]
    
    def mkdir(self, path, mode):
        """Make a new directory."""
        # Add directory on endpoint.
        _, _, data = self.api.endpoint_mkdir(self.endpoint, path)
        print data['message']
        # Add directory entries in memory.
        self.dirs[path] = []
        self.dirs[os.path.dirname(path)].append(os.path.basename(path))
        # Add file entry in memory.
        now = time.time()
        self.files[path] = {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                            'st_nlink': 2, 'st_mode': (stat.S_IFDIR | 0755)}

    def readdir(self, path, fh):
        """List contents of a directory (e.g. from ls)."""
        self._LoadDir(path)
        return ['.', '..'] + self.dirs[path]

    def rename(self, old, new):
        """Rename a file/directory by submitting a transfer."""
        raise FuseOSError(EROFS)
        # TODO: is there an API call to do this more efficiently?
        # TODO: will this cause asynchronous issues?
        # Copy file to new location.
        # self._Copy(old, new)
        # self._Delete(old)

    def rmdir(self, path):
        """Remove an empty directory."""
        self._LoadDir(path)
        if self.dirs[path]:
            # Directory not empty.
            raise FuseOSError(errno.ENOTEMPTY)
        # Submit task to remove directory from endpoint.
        self._Delete(path)
        # Remove entry from the saved metadata.
        self.dirs[os.path.dirname(path)].remove(os.path.basename(path))

    def unlink(self, path):
        """Unlink (remove) a file."""
        self._Delete(path)
        # Remove entry from the saved metadata.
        self.dirs[os.path.dirname(path)].remove(os.path.basename(path))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('endpoint', help='Globus endoint name')
    parser.add_argument('mountpoint', help='Local mount path')
    args = parser.parse_args()

    if os.geteuid() != 0:
        exit('You must run as root to use FUSE.')

    FUSE(GlobusFS(args.endpoint), args.mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main()