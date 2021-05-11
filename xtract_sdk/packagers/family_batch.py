
from .family import Family


class FamilyBatch:
    def __init__(self):
        self.families = []
        self.file_ls = []

    def add_family(self, family):
        self.families.append(family)
        self.file_ls.extend(family.files)

    def to_dict(self):
        the_dict = {'files': self.file_ls, 'families': [family.to_dict() for family in self.families]}
        return the_dict

    def from_dict(self, dict_obj):

        # Since a familybatch is just a collection of families and files...
        for family in dict_obj['families']:
            fam = Family()
            fam.from_dict(family)

            self.families.append(fam)
        print(self.families)
        self.file_ls = dict_obj['files']
