# myfunctions
"""
A file containing the functions needed for the ETL process of the NBADW.
"""


import os
import kaggle


def set_directory_to_nbadw(directory='NBADW'):
    """Uses the 'os' package to set the current working directory to the project
    root folder.
    """
    dir_variable = os.path.normpath(os.getcwd())
    if os.path.basename(dir_variable) == directory:
        print(f'Working in {dir_variable}')
        return
    else:
        os.chdir("D:\\Users\\Home\\source\\repos\\leetroyjenkins\\NBADW")
        dir_variable = os.path.normpath(os.getcwd())
        print(f'Working in {dir_variable}')
        return


def kaggle_download_and_unzip(dataset_list, save_path):
    """Downloads and unzips kaggle files from datasets. Datasets are identified
    in the dataset_list and saved to the save_path location.

    Requires Kaggle API installed in C:\Users\[user]\.kaggle
    """
    kaggle.api.authenticate()
    print('Downloading datasets. This will take a bit.')
    for dataset in dataset_list:
        print(f'downloading {dataset}')
        kaggle.api.dataset_download_files(dataset, path=save_path, unzip=True)
        print(f'{dataset} downloaded')
