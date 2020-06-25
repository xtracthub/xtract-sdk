
from uuid import uuid4
from .group import Group


class Family:
    def __init__(self, family_id=None, headers=None, metadata=None):
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
        self.files = set()

    def add_group(self, files, parser):

        group_id = str(uuid4())

        for filename in files:
            self.files.add(filename)

        self.groups[group_id] = Group(group_id, files, parser, metadata=None)
        return group_id

    def to_dict(self):
        fam_dict = {'family_id': self.family_id,
                  'headers': self.headers,
                  'metadata': self.metadata,
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

        raw_groups = fam_dict["groups"]
        for group in raw_groups:
            self.groups[group["group_id"]] = Group(group_id=group["group_id"],
                                       files=group["files"],
                                       parser=group["parser"],
                                       metadata=group["metadata"])





