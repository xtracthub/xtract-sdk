
from abc import ABCMeta, abstractmethod
import tempfile
import os


class DownloaderInterface(metaclass=ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'fetch') and
                callable(subclass.fetch) and
                hasattr(subclass, 'batch_fetch') and
                callable(subclass.batch_fetch) or NotImplemented)

    def __init__(self, download_type="file", mkdir=False):
        self.download_type = download_type
        self.orig_dir = os.getcwd()

        if mkdir:
            self.new_dir = tempfile.mkdtemp()
        else:
            self.new_dir = self.orig_dir

        self.success_files = []
        self.fail_files = []

    @abstractmethod
    def fetch(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def batch_fetch(self, *args, **kwargs):
        raise NotImplementedError
