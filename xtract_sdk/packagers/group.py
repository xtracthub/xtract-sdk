
class Group:
    def __init__(self, group_id, files, parser, metadata=None):

        self.group_id = group_id

        assert(type(files) is list)
        self.files = files

        # NOTE that parser can be None as it will be indexed as such later on.
        assert type(parser) is str or parser is None, f"Parser is of improper type: {type(parser)}"
        self.parser = parser

        assert type(metadata) is dict or metadata is None, "Group metadata is not of type 'dict' or None"
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata

    def update_metadata(self, metadata):
        self.metadata = metadata
