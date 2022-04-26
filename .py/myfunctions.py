#myfunctions
'''
A file containing the functions needed for the ETL process of the NBADW.

'''


import os


def setDirectoryToNBADW(directory='NBADW'):
    dir = os.path.normpath(os.getcwd())
    if os.path.basename(dir) == directory:
        print(f'Already working in {dir}')
        return
    else:
        dir = os.chdir("D:\\Users\\Home\\source\\repos\\leetroyjenkins\\NBADW")
        print(f'Updated working directory to {dir}')
        return

