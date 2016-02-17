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
from globusonline.transfer.api_client import goauth, TransferAPIClient


class GlobusFS(Operations):

    def __init__(self, endpoint):
        # Get credentials and activate endpoint.
        auth_result = goauth.get_access_token()
        self.api = TransferAPIClient(username=auth_result.username, goauth=auth_result.token)
        self.endpoint = endpoint
        _, _, data = self.api.endpoint_autoactivate(endpoint)
        print data['message']

        # Cache file metadata in memory.
        now = time.time()
        root_stat = {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                     'st_nlink': 2, 'st_mode': (stat.S_IFDIR | 0755)}
        self.files = {'/': root_stat}  # Map filepath to stat() file info dictionaries.
        self.dirs = {}  # Map dirname to list of filenames.


    def access(self, path, amode):
        """Pre-emptively read file metadata when accessing a new directory."""
        print 'access', path, amode
        if path not in self.dirs:
            _, _, data = self.api.endpoint_ls(self.endpoint, path=path)

            # Add list of file names to directory (for readdir())
            self.dirs[path] = [x['name'] for x in data['DATA']]

            # Add file metadata (for getattr())
            for file_info in data['DATA']:
                now = time.time()
                f_type = stat.S_IFDIR if file_info['type'] == 'dir' else stat.S_IFREG
                permissions = int(file_info['permissions'], 8)  # permissions are octal

                self.files[os.path.join(path, file_info['name'])] = {
                    'st_atime': now,
                    'st_mtime': now,  # TODO: last_modified is an actual field we can use
                    'st_ctime': now,
                    'st_nlink': 2,
                    'st_mode': (f_type | permissions),
                    'st_size': file_info['size']
                }
        return 0

    # TODO: what if the first thing we do is ls a subdirectory
    # TODO: mkdir: need to add entry to the parent directory

    def getattr(self, path, fh=None):
        print 'getattr', path
        if path not in self.files:
            raise FuseOSError(errno.ENOENT)
        return self.files[path]
    
    def mkdir(self, path, mode):
        print 'mkdir', path
        self.api.endpoint_mkdir(self.endpoint, path)
        self.dirs[path] = []
        now = time.time()
        self.files[path] = {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                            'st_nlink': 2, 'st_mode': (stat.S_IFDIR | 0755)}

    def readdir(self, path, fh):
        print 'readdir', path
        return ['.', '..'] + self.dirs[path]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('endpoint', help='Globus endoint name')
    parser.add_argument('mountpoint', help='Local mount point')
    args = parser.parse_args()

    if os.geteuid() != 0:
        exit('You must run as root to use FUSE.')

    FUSE(GlobusFS(args.endpoint), args.mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main()