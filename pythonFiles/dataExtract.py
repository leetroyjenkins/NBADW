'''
This file contains the functions for extracting data from Kaggle and the other
sources needed for the NBADW
'''

import kaggle
import os

import nba_api.stats.endpoints
import pandas as pd
from myfunctions import *
from nba_api.stats.endpoints import CommonAllPlayers
import requests



# Sets directory to the root folder for the NBADW project.
setDirectoryToNBADW()

cwd = os.getcwd()
cwd = os.getcwd()
source_dir = cwd + "\\DataFiles\\Source_Data\\"
data_dir = cwd + '\\DataFiles\\'

###############################################################################
# Download datasets from Kaggle
# Requires Kaggle API installed in C:\Users\[user]\.kaggle
###############################################################################

# List of datasets/files to download
download_list = ['patrickhallila1994/nba-data-from-basketball-reference',
                 'boonpalipatana/nba-season-records-from-every-year']

kaggle.api.authenticate()
print('Downloading datasets. This will take a bit.')

for i in download_list:
    kaggle.api.dataset_download_files(i, path=source_dir, unzip=True)

print('Kaggle Datasets extracted.')

# Use the NBA API to get a list of all players from stats.nba.com
#nba_players2 = CommonAllPlayers()

#nba_players = nba_players2['']