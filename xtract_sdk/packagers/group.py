
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
