#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
import sys
import json

result_path = './vm_result.csv'
test_headers = ['index', 'timestamp','vmid','mincpu', 'maxcpu_x', 'avgcpu_x', 'vmmemory']
test_df = pd.read_csv(result_path, header=None, index_col=False, names=test_headers, delimiter=',')
vmids = list(set(test_df['vmid'].values.tolist()))
vmids.remove('vmid')
result = {}
count = 0
for vmid in vmids:
    count = count + 1
    if vmid == 'vmid':
        continue
    result = []
    rows = test_df.loc[(test_df['vmid'] == vmid)]
    if len(rows['avgcpu_x'].values.tolist()) > 70:
        with open('./azure_trace_data.csv', 'a') as f:
            f.write('%s\n' % ','.join(rows['avgcpu_x'].values.tolist()))
    
