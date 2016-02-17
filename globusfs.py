#!/usr/bin/env python

# Copyright (c) 2016 Austin Byers <austin.b.byers@gmail.com>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.

"""Mount a Globus endpoint over FUSE."""


import argparse
import os
import stat
import time

from fuse import FUSE, FuseOSError, Operations
from globusonline.transfer.api_client import goauth, TransferAPIClient


class GlobusFS(Operations):

    def __init__(self, endpoint):
        # Get credentials and activate endpoint.
        auth_result = goauth.get_access_token()  # Don't keep access token around in memory longer than needed.
        self.api = TransferAPIClient(username=auth_result.username, goauth=auth_result.token)
        self.endpoint = endpoint
        self.api.endpoint_autoactivate(endpoint)

    def getattr(self, path, fh=None):
        print 'getattr', path
        now = time.time()
        return {'st_atime': now, 'st_mtime': now, 'st_ctime': now,
                'st_nlink': 2, 'st_mode': (stat.S_IFDIR | 0755)}
    
    def mkdir(self, path, mode):
        print 'mkdir: ', path
        self.api.endpoint_mkdir(self.endpoint, path)

    def readdir(self, path, fh):
        code, msg, data = self.api.endpoint_ls(self.endpoint)
        return [x['name'] for x in data['DATA']]


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