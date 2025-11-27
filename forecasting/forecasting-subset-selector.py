

import glob
from pathlib import Path
import shutil
import os

path = 'C:/Users/Solar/Documents/GitHub/solar-resource-course/forecasting/'

os.chdir(path)

files = glob.glob('data/*T0600.csv')

new_folder = 'data-daily'

for src_path in files:
    dest_path = new_folder + '/' + Path(src_path).name
    shutil.copy(src_path, dest_path)
