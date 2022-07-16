from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from posixpath import split
import pandas as pd
import numpy as np
import time
import sys

src_file  = str(sys.argv[1])
dest_file = str(sys.argv[2])

# get the start time
st = time.time()

def add_6_hour(time_str):    
    date_format_str = '%Y-%m-%d %H:%M:%S.%f'
    given_time = datetime.strptime(time_str, date_format_str)
    n = 6
    final_time = given_time + timedelta(hours=n)
    final_time = final_time.strftime("%Y-%m-%d %H:%M:%S")
    final_time_str = str(final_time)
    return final_time_str

with open(src_file) as f:
    lines = f.readlines()

i = 0
df = pd.DataFrame(columns=['id', 'text', 'time'])

for line in lines:
    if '800A' in line:
        _time = line.split()[1] + ' ' + line.split()[2]
        _time = add_6_hour(_time)
        
        if '\', \'text\': \'' in lines[i-1]:
            _split = lines[i-1].split('\', \'text\': \'')
            _id     = _split[0]
            _id     = _id.replace('{\'id\': \'', '')        
            _text   = _split[1]
            _text   = _text.replace('\'}\n', '')  
        else:
            _split = lines[i-1].split('\', \'text\': \"')
            _id     = _split[0]
            _id     = _id.replace('{\'id\': \'', '')        
            _text   = _split[1]
            _text   = _text.replace('\"}\n', '')  
  

        list_row = [_id, _text, _time]
        df.loc[len(df)] = list_row
    i += 1

df.to_csv(dest_file, index=False, sep=';')

et = time.time()
elapsed_time = et - st
print('Execution time:', elapsed_time, 'seconds')