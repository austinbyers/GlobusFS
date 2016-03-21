"""Interact with the Globus API."""
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

    def _SubmissionID(self):
        """Get a new submission id."""
        status, msg, data= self.api.transfer_submission_id()
        return data['value']

    def _Copy(self, src_path, dest_path):
        """Submit a task to recursively copy to and from the same endpoint."""
        # TODO: should be recursive only if directory
        rename_task = api_client.Transfer(
            self._SubmissionID(), self.remote_endpoint, self.remote_endpoint)
        rename_task.add_item(src_path, dest_path, recursive=True)
        status, msg, data = self.api.transfer(rename_task)
        print data['message']

    def CopyToLocal(self, remote_path, local_path, timeout=10):
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
            self._SubmissionID(), self.remote_endpoint, self.local_endpoint)
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

    def Delete(self, path):
        """Add a task to recursively delete the given path."""
        delete_task = api_client.Delete(self._SubmissionID(), self.remote_endpoint, recursive=True)
        delete_task.add_item(path)
        _, _, data = self.api.delete(delete_task)
        print data['message']

    def EndpointList(self, path):
        """Return a list of file info dictionaries for the given path."""
        print 'Loading directory %s from Globus...' % path
        status, msg, data = self.api.endpoint_ls(self.remote_endpoint, path=path)
        return data['DATA']

    def Mkdir(self, path):
        """Make a directory on the remote endpoint."""
        _, _, data = self.api.endpoint_mkdir(self.remote_endpoint, path)
        print data['message']