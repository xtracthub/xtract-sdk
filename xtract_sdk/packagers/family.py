
from uuid import uuid4
from .group import Group


class Family:
    def __init__(self, download_type=None, family_id=None, headers=None, metadata=None, base_url=""):

        self.download_type = download_type

        if not family_id:
            self.family_id = str(uuid4())
        else:
            self.family_id = family_id

        self.headers = headers
        self.groups = dict()

        assert type(metadata) is dict or metadata is None, "Family metadata is not of type 'dict' or None"
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata
        self.file_paths = set()
        self.files = []

        self.base_url = base_url

    def add_group(self, files, parser, metadata=None):
        group_id = str(uuid4())

        for file_dict in files:
            assert type(file_dict) is dict, "File not given in dict form {'path': <str>, 'metadata': <mdata>}"
            if file_dict['path'] not in self.file_paths:
                self.file_paths.add(file_dict['path'])
                file_dict["file_id"] = str(uuid4())
                self.files.append(file_dict)

        self.groups[group_id] = Group(group_id, files, parser, metadata)
        return group_id

    def to_dict(self):
        fam_dict = {'family_id': self.family_id,
                    'headers': self.headers,
                    'metadata': self.metadata,
                    'download_type': self.download_type,
                    'files': self.files,
                    'base_url': self.base_url,
                    'groups':
                    [{'group_id': self.groups[group].group_id,
                      'files': self.groups[group].files,
                      'parser': self.groups[group].parser,
                      'metadata': self.groups[group].metadata}
                     for group in self.groups]}
        return fam_dict

    def from_dict(self, fam_dict):
        self.family_id = fam_dict["family_id"]
        self.headers = fam_dict["headers"]
        self.metadata = fam_dict["metadata"]
        self.download_type = fam_dict["download_type"]
        self.files = fam_dict["files"]
        self.base_url = fam_dict["base_url"]

        raw_groups = fam_dict["groups"]
        for group in raw_groups:
            self.groups[group["group_id"]] = Group(group_id=group["group_id"],
                                                   files=group["files"],
                                                   parser=group["parser"],
                                                   metadata=group["metadata"])
