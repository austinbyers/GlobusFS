# GlobusFS
Mount a Globus endpoint over FUSE.

This is a work-in-progress which should be ready by mid-March, 2016.

## Setup
The only thing you need (besides a Globus account) is the Globus Python API:

``pip install globusonline-transfer-api-client``

GlobusFS is built on [fusepy](https://github.com/terencehonles/fusepy), which
is already included in the repo (``fuse.py``).

## Usage
Note that you must be root to use FUSE.

Right now, supported commands are ``cd``, ``ls``, ``rm``, ``mkdir``, and ``rmdir``.

Mounting an endpoint:
``mkdir mnt``
``sudo python globusfs.py 'go#ep1' mnt``

This process will continue running to respond to fs events.
You can explore the mount point in a new terminal window:

``sudo su; cd mnt``