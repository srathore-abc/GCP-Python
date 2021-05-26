import glob
import os
for filepath in glob.iglob('replacefiles/*.yaml', recursive=True):
    with open(filepath) as file:
        s = file.read()
    s = s.replace('/pe/', '/co/')
    s = s.replace('PE_', 'CO_')
    s = s.replace('pe_', 'co_')
    s = s.replace('pe', 'co')

    with open(filepath, "w") as file:
        file.write(s)

