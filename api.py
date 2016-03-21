"""Interact with the Globus API. All endpoint connections happen here."""
import threading
import time

from globusonline.transfer import api_client


class GlobusAPI(object):

    def __init__(self, local_endpoint, remote_endpoint):
        """Create a wrapper around the Globus API Client."""
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

        # Setup asynchronous task queue.
        self.task_queue = AsyncTaskQueue(self)

    def Close(self):
        """Wait for pending changes and close out the API connection."""
        self.task_queue.Finish()
        self.api.close()

    def SubmissionID(self):
        """Get a new submission id."""
        status, msg, data= self.api.transfer_submission_id()
        return data['value']

    ######################
    #  Blocking Requests #
    ######################

    def CopyToLocal(self, remote_path, local_path, timeout_secs=10):
        """Copy a remote file into the local endpoint.

        Args:
            remote_path: Remote file path to copy.
            local_path: Destination file path.
            timeout: Maximum waiting time (in seconds) for file transfer to complete.

        Returns:
            True if the transfer was successful, False otherwise.
        """
        # Copy the file over the network; block until successful or timeout.
        print 'Copying {0} to local cache...'.format(remote_path)
        task = api_client.Transfer(
            self.SubmissionID(), self.remote_endpoint, self.local_endpoint)
        task.add_item(remote_path, local_path)
        status, msg, data = self.api.transfer(task)
        task_id = data['task_id']

        success = False
        for _ in xrange(timeout):
            status, msg, data = self.api.task(task_id)
            if data['completion_time']:
                success = True
                break
            time.sleep(1)

        return success

    def EndpointList(self, path):
        """Return a list of file info dictionaries for the given path."""
        print 'Loading directory %s from Globus...' % path
        status, msg, data = self.api.endpoint_ls(self.remote_endpoint, path=path)
        return data['DATA']

    def Mkdir(self, path):
        """Make a directory on the remote endpoint."""
        _, _, data = self.api.endpoint_mkdir(self.remote_endpoint, path)
        print data['message']

    ########################
    #  Background Requests #
    ########################

    def Delete(self, path):
        """Add a task to recursively delete the given path."""
        self.task_queue.AddDeletion(self.remote_endpoint, path)

    def Rename(self, old_path, new_path):
        """Move/Rename a file on the remote endpoint."""
        # Copy the file to its new location.
        rename_task = api_client.Transfer(
            self.SubmissionID(), self.remote_endpoint, self.remote_endpoint)
        rename_task.add_item(old_path, new_path, recursive=True)
        status, msg, data = self.api.transfer(rename_task)
        print data['message']

        # Then delete the original.
        self.Delete(old_path)


class AsyncTaskQueue(object):
    """Asynchronous task queue. This allows us to batch related requests together.

    For example, recurisvely removing a directory would make dozens of calls to api.Delete().
    Rather than sending the requests individually, this class allows us to batch them together,
    reducing network overhead and improving performance.
    """

    def __init__(self, api):
        # Store tasks as a list (queue).
        # Each entry is a 2-tuple:
        #     descriptor tuple e.g. ('delete', 'go#ep1')
        #     Globus api_client task to submit
        self.queue = []

        self.api = api  # GlobusAPI() object.
        self.direct_api = api.api  # Underlying api object.
        self.lock = threading.Lock()
        self.last_change = time.time()  # Time of last task submission.
        self.closing = False  # Flag to indicate when the process should close.
        self.handler_thread = threading.Thread(target=self.HandleTasks)
        self.handler_thread.start()

    def Finish(self):
        """Wait until all pending changes have synced and the thread quits."""
        self.closing = True
        self.handler_thread.join()

    def HandleTasks(self):
        """Async function: wake up every so often and process the pending tasks."""
        while True:
            with self.lock:
                if self.closing or time.time() - self.last_change > 5:
                    # We're closing or the last change was more than 5 seconds ago; push changes.
                    print 'Clearing task queue...'
                    for descriptor, task in self.queue:
                        if descriptor[0] == 'delete':
                            _, _, data = self.direct_api.delete(task)
                            print 't' + data['message']
                    self.queue = []
            if self.closing:
                return
            time.sleep(2)

    def AddDeletion(self, endpoint, path):
        descriptor = ('delete', endpoint)
        with self.lock:
            if self.queue and self.queue[-1][0] == descriptor:
                self.queue[-1][1].add_item(path)
            else:
                task = api_client.Delete(self.api.SubmissionID(), endpoint, recursive=True)
                self.queue.append((descriptor, task))
            self.last_change = time.time()