#myfunctions
'''
A file containing the functions needed for the ETL process of the NBADW.

'''

import os


def setDirectoryToNBADW(directory='NBADW'):
    dir = os.path.normpath(os.getcwd())
    if os.path.basename(dir) == directory:
        print(f'Working in {dir}')
        #return
    else:
        os.chdir("D:\\Users\\Home\\source\\repos\\leetroyjenkins\\NBADW")
        dir = os.path.normpath(os.getcwd())
        print(f'Working in {dir}')
        #return

