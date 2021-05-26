#Rename File Names
import os
pathiter = (os.path.join(root, filename)
    for root, _, filenames in os.walk("replacefiles")
    for filename in filenames
)
for path in pathiter:
    newname =  path.replace('pe_', 'co_')
    if newname != path:
        os.rename(path,newname)