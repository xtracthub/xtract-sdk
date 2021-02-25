
import os
import time
import requests
import tempfile
import threading
from queue import Queue


class GlobusHttpsDownloader:
    def __init__(self, download_type="file", in_tmpdir=False):

        self.download_type = download_type
        self.orig_dir = os.getcwd()

        if in_tmpdir:
            self.new_dir = tempfile.mkdtemp()
        else:
            self.new_dir = self.orig_dir

        # TODO: should really use the built-ins for these (from base.py class)
        self.success_files = []
        self.fail_files = []

    def fetch(self, remote_filepath, headers, rel_loc_path):

        try:
            req = requests.get(remote_filepath, headers=headers)
        except Exception as e:
            self.fail_files.append(remote_filepath)
            raise Exception(f"[Xtract] Unable to fetch HTTPS file. Caught: {e}")

        head, tail = os.path.split(rel_loc_path)
        actual_directory = os.path.join(self.new_dir, head)
        actual_full_file_path = os.path.join(actual_directory, tail)

        os.makedirs(actual_directory, exist_ok=True)

        with open(actual_full_file_path, 'wb') as f:
            f.write(req.content)
        self.success_files.append({'remote_path': remote_filepath, 'local_path': actual_full_file_path})

    def batch_fetch(self, remote_filepaths, num_threads=2):
        """
        :param remote_filepaths (tuple) of form (remote_filepath, local_filepath, headers)
        :param headers:
        :param num_threads:
        :return: None (put onto self.success_queue)
        """

        q = Queue()
        for filepath in remote_filepaths:
            q.put(filepath)

        num_threads = max(num_threads, len(remote_filepaths))
        # e.g., if there are fewer files than max_threads, we would have idle threads -- hence, THIS!

        total_thread_counter = 0
        num_active_threads = 0
        all_thread_ls = []
        active_thread_queue = Queue()
        while not q.empty():

            if num_active_threads < num_threads:
                remote_filepath, local_filename, headers = q.get()
                thr = threading.Thread(target=self.fetch, args=(remote_filepath, headers, local_filename))
                total_thread_counter += 1
                num_active_threads += 1
                thr.start()

                active_thread_queue.put(thr)
                all_thread_ls.append(thr)

            new_spots = 0
            for i in range(num_active_threads):
                thr = active_thread_queue.get()
                if thr.is_alive():
                    active_thread_queue.put(thr)
                else:
                    num_active_threads -= 1
                    new_spots = 1
            # Avoid cycling the CPU by sleeping for brief period if nothing has progressed.
            if new_spots == 0:
                time.sleep(0.5)  # TODO: Taking suggestions for better ways of avoiding CPU cycling.

        for thr in all_thread_ls:
            thr.join()
