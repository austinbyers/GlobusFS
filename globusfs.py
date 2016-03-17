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
import pprint  # TODO: temporary
import shutil
import stat
import time

from fuse import FUSE, FuseOSError, Operations
from globusonline.transfer import api_client


class GlobusFS(Operations):

    # TODO: encryption
    # TODO: tests
    # TODO: queue requests into batches (e.g. rm -r should not send so many reqs)
    # TODO: we probably must update cache every now and then to pull updates

    def __init__(self, local_endpoint, local_path, remote_endpoint):
        """Initialize the FUSE wrapper.

        Args:
            local_endpoint: Name of the Globus endpoint running locally.
            local_path: Directory visible to the local endpoint. Cache will be stored under this
                directory.
            remote_endpoint: Name of the remote endpoint to be mounted.
        """
        # Get credentials.
        auth_result = api_client.goauth.get_access_token()
        self.api = api_client.TransferAPIClient(
            username=auth_result.username, goauth=auth_result.token)

        # Activate endpoints.
        self.local_endpoint, self.remote_endpoint = local_endpoint, remote_endpoint
        status, msg, data = self.api.endpoint_autoactivate(local_endpoint)
        print data['message']
        assert status == 200
        status, msg, data = self.api.endpoint_autoactivate(remote_endpoint)
        print data['message']
        assert status == 200

        # Cache file metadata in memory.
        now = time.time()
        root_stat = {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                     'st_nlink': 2, 'st_mode': (stat.S_IFDIR | 0755)}
        self.files = {'/': root_stat}  # Map filepath to stat() file info dictionaries.
        self.dirs = {}  # Map dirname to list of filenames.

        # Cache file data on disk.
        self.cache_dir = os.path.join(local_path, '.globusfs-cache')
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)
        os.mkdir(self.cache_dir)
        os.chmod(self.cache_dir, 0777)
        self.cache = {}  # Maps filepath to file object, or None if the file isn't yet open.

    def _LoadDir(self, path):
        """Load directory information from endpoint if it isn't in memory already."""
        if path in self.dirs:
            return

        print '\t--> Loading directory %s from Globus' % path
        _, _, data = self.api.endpoint_ls(self.remote_endpoint, path=path)

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

    def _SubmissionID(self):
        """Get a new submission id."""
        status, msg, result = self.api.transfer_submission_id()
        assert status == 200
        return result['value']

    def _Copy(self, src_path, dest_path):
        """Submit a task to recursively copy to and from the same endpoint."""
        # TODO: should be recursive only if directory
        rename_task = api_client.Transfer(
            self._SubmissionID(), self.remote_endpoint, self.remote_endpoint)
        rename_task.add_item(src_path, dest_path, recursive=True)
        _, _, data = self.api.transfer(rename_task)
        print data['message']

    def _Delete(self, path):
        """Submit a task to recursively delete the given path."""
        delete_task = api_client.Delete(self._SubmissionID(), self.remote_endpoint, recursive=True)
        delete_task.add_item(path)
        _, _, data = self.api.delete(delete_task)
        print data['message']

    ####################
    #    FS Commands   #
    ####################

    def destroy(self, path):
        """Called on filesystem destruction. Path is always /"""
        shutil.rmtree(self.cache_dir)
        print 'Local cache destroyed'

    def flush(self, path, fh):
        """Flush the internal I/O buffer when reading a file."""
        self.cache[path].flush()
        return 0

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
        _, _, data = self.api.endpoint_mkdir(self.remote_endpoint, path)
        print data['message']
        # Add directory entries in memory.
        self.dirs[path] = []
        self.dirs[os.path.dirname(path)].append(os.path.basename(path))
        # Add file entry in memory.
        now = time.time()
        self.files[path] = {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                            'st_nlink': 2, 'st_mode': (stat.S_IFDIR | 0755)}

    def open(self, path, flags):
        """Open a file. This will require copying to the disk cache if we haven't already."""
        # Grab the file over the network if necessary.
        cache_file = os.path.join(self.cache_dir, os.path.basename(path))
        if path not in self.cache:
            # Copy the file over the network; block until successful or timeout.
            print 'Copying {0} to local cache...'.format(path)
            task = api_client.Transfer(
                self._SubmissionID(), self.remote_endpoint, self.local_endpoint)
            task.add_item(path, cache_file)
            status, msg, data = self.api.transfer(task)
            assert status == 202
            task_id = data['task_id']

            success = False
            for _ in xrange(10):
                status, msg, data = self.api.task(task_id)
                if data['completion_time']:
                    success = True
                    break
                time.sleep(1)

            if not success:
                # Timeout - TODO: cancel task
                raise FuseOSError(EROFS)

        # Open the file and return a file handle.
        f = self.cache[path] = open(cache_file, mode='r+')
        return f.fileno()

    def read(self, path, size, offset, fh):
        """Returns a string containing the file data requested."""
        f = self.cache[path]
        f.seek(offset)
        return f.read(size)

    def readdir(self, path, fh):
        """List contents of a directory (e.g. from ls)."""
        self._LoadDir(path)
        return ['.', '..'] + self.dirs[path]

    def release(self, path, fh):
        """Release a file after reading it."""
        self.cache[path].close()

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
        del self.files[path]

    def unlink(self, path):
        """Unlink (remove) a file."""
        self._Delete(path)
        # Remove entry from the saved metadata.
        self.dirs[os.path.dirname(path)].remove(os.path.basename(path))

def main():
    # TODO: Note that the local endpoint needs to be the "legacy name"
    # TODO: add mode without local endpoint - to just browse / reorganize files
    # TODO: by default, it should be assumed the home directory is accessible.
    # TODO: determine local endpoint name automagically
    parser = argparse.ArgumentParser()
    parser.add_argument('local_endpoint', help='Name of the Globus endpoint running locally')
    parser.add_argument(
        'cache_dir', help='Directory visible from the local endpoint. Cache will be stored here.')
    parser.add_argument('remote_endpoint', help='Globus endoint name')
    parser.add_argument('mountpoint', help='Local mount path')
    args = parser.parse_args()

    if os.geteuid() != 0:
        exit('You must run as root to use FUSE.')

    globus_fs = GlobusFS(args.local_endpoint, args.cache_dir, args.remote_endpoint)
    FUSE(globus_fs, args.mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main()