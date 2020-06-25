
from uuid import uuid4


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
                  'groups':
                      [{'group_id': self.groups[group].group_id,
                        'files': self.groups[group].files,
                        'parser': self.groups[group].parser}
                       for group in self.groups]}
        return fam_dict

    def load_dict(self, fam_dict):
        print(fam_dict)


class Group:
    def __init__(self, group_id, files, parser, metadata=None):

        self.group_id = group_id

        assert(type(files) is list)
        self.files = files

        assert(type(parser) is str)
        self.parser = parser

        assert type(metadata) is dict or metadata is None, "Group metadata is not of type 'dict' or None"
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata
