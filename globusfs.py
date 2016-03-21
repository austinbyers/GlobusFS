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

from fuse import FUSE, FuseOSError, Operations

import api
import cache


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
        # Wrapper around the globus API.
        self.api = api.GlobusAPI(local_endpoint, remote_endpoint)

        # Cache file metadata in memory.
        self.metadata = cache.MetaData(self.api)

        # Cache file data in local endpoint.
        self.file_cache = cache.FileCache(self.api, local_path)

    ####################
    #    FS Commands   #
    ####################

    def create(self, path, mode, fi=None):
        """Create a new file."""
        self.file_cache.Create(path)
        self.metadata.NewFile(path, mode)
        return 0

    def destroy(self, path):
        """Called on filesystem destruction. Path is always /"""
        self.file_cache.Destroy()

    def flush(self, path, fh):
        """Flush the internal I/O buffer when reading a file."""
        self.file_cache.Get(path).flush()
        return 0

    def getattr(self, path, fh=None):
        """Get metadata for a specific file/directory."""
        stat = self.metadata.Stat(path)
        if stat:
            return stat
        else:
            # File doesn't exist.
            raise FuseOSError(errno.ENOENT)
    
    def mkdir(self, path, mode):
        """Make a new directory."""
        self.api.Mkdir(path)
        self.metadata.NewDirectory(path)

    def open(self, path, flags):
        """Open a file. This will require copying to the disk cache if we haven't already."""
        f = self.file_cache.Open(path, flags)
        if f:
            return f.fileno()
        else:
            # File timeout or other problem.
            return FuseOSError(errno.EROFS)

    def read(self, path, size, offset, fh):
        """Returns a string containing the file data requested."""
        f = self.file_cache.Get(path)
        f.seek(offset)
        return f.read(size)

    def readdir(self, path, fh):
        """List contents of a directory (e.g. from ls)."""
        return self.metadata.Listdir(path)

    def release(self, path, fh):
        """Release a file after reading it."""
        self.file_cache.Get(path).close()
        return 0

    def rename(self, old, new):
        """Rename a file/directory by submitting a transfer."""
        # TODO: is there an API call to do this more efficiently?
        # TODO: will this cause asynchronous issues?
        # Copy file to new location.
        #self._Copy(old, new)
        #self._Delete(old)
        self.metadata.Rename(old, new)

    def rmdir(self, path):
        """Remove an empty directory."""
        if len(self.metadata.Listdir(path)) > 2:
            # Directory not empty.
            raise FuseOSError(errno.ENOTEMPTY)
        return self.unlink(path)

    def unlink(self, path):
        """Unlink (remove) a file."""
        self.api.Delete(path)
        self.metadata.Remove(path)
        return 0

    def write(self, path, data, offset, fh):
        """Write data to a file."""
        # Write data to the local cache.
        f = self.file_cache.Get(path)
        f.seek(offset)
        f.write(data)
        # Update file size.
        f_size = self.metadata.Stat(path)['st_size']
        self.metadata.ChangeFileSize(path, max(f_size, offset + len(data)))
        return len(data)


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