# GlobusFS
Mount a Globus endpoint over FUSE.

This allows you to explore a Globus endpoint as if it were a regular
filesystem attached to your local computer. This might be more useful than the Globus web interface for:

* Making lots of changes (e.g. reorganizing the remote directory structure).
* Running complex bash scripts without access to the remote machine.
* Mounting multiple endpoints, which would provide an overview of multiple systems.

In addition, the code itself may be useful as an example of using the Globus Python API.


## Disclaimer
This is not an official Globus product, nor has it been rigorously tested. Only basic filesystem
operations have thus far been implemented (e.g. basic traversal and reads).
For now, GlobusFS is primarily an educational exercise that allows you to have read access to a
remote Globus endpoint.

PLEASE DO NOT USE GLOBUSFS WITH IMPORTANT DATA; lest it accidentally be deleted.
If GlobusFS sounds like something that would actually be useful for you, I recommend contacting
me or else spending some time to flesh out the code.


## Setup
The only dependency is the Globus Python API:

``pip install globusonline-transfer-api-client``

GlobusFS is built on [fusepy](https://github.com/terencehonles/fusepy), which
is already included in the repo (``fuse.py``).

In order to open files on a remote endpoint, you must first be able to transfer them to your
local computer. Globus only understands whole-file transfers between two active endpoints.
Thus, you will need [Globus Connect Personal](https://www.globus.org/globus-connect-personal)
running on your local machine.


## Usage
Note that you must be root to use FUSE.

Mounting an endpoint:
```
mkdir mnt
sudo python globusfs.py local-endpoint cache-directory remote-endpoint mnt
```

    1. ``local-endpoint`` is the *legacy name* of your local endpoint. This should look something like
 'username#7f59803e-ebc7-11e5-9829-a2000b9d545e' and can be found in the details of the
  Endpoints section of the Globus web app.
    2. ``cache-directory`` is the path to a folder in your local endpoint where transferred files
    will be cached. Any directory in the local endpoint will work, GlobusFS just needs a local
    filepath that is connected to an endpoint.
    3. ``remote-endpoint`` is the name of the remote endpoint to connect to, e.g. 'go#ep1`
    4. ``mnt`` is the path to the directory where the filesystem will be mounted.

This process will continue running to respond to fs events.
You can explore the mount point in a new terminal window:

``sudo su; cd mnt``

The only commands that have been tested are ``cd``, ``ls``, ``cp``, ``rm`` and ``cat``.
In other words, navigation and file reads.


## Example
Suppose my computer is endpoint ``austin#123`` and it's connected to ``~/globus-endpoint.``
Then I can mount the Globus tutorial endpoint as follows:
``sudo python globusfs.py austin#123 ~/globus-endpoint go#ep1 mnt``


## Caching
In order to work effectively (and minimize the number of network calls), file metadata is cached in
memory and file data is cached in the local endpoint and asynchronously updated.
Changes made to the remote endpoint outside GlobusFS will not be reflected until it has been remounted.

Caching has important implications for large files: if you try to open a large file,
it must first transfer the entire file to your local computer. Similarly, if you copy a file
from the remote endpoint into an arbitrary directory on the local computer, the file must first
be sent to the local endpoint. Thus, there will actually be 2 copies on the local machine.
These limitations are inherent in the way that FUSE intercepts low-level filesystem calls and in
the simplicity of the Globus API.


## TODO
If GlobusFS were to see actual use, it would need the following:
  * Proper ordering and status-checking for API calls (e.g. a copy followed by a delete must happen
    in that order; we need to wait for the first operation to finish).
  * Writes are never pushed to the remote endpoint.
  * Tests
  * Support for links
  * Cache replacement? E.g. LRU
  * Program options (encryption, timeouts, max cache size, etc).