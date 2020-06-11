
from xtract_sdk.downloaders.figshare import FigshareDownloader

fs = FigshareDownloader("file", False)
fid = 1057646
path = '/Users/tylerskluzacek/Desktop/'
fs.fetch(fid, path)