#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
import sys
import json
'''
vmtable_path = './vmtables/vmtable.csv'
vmtable_headers = ['vmid','subscriptionid', 'deploymentid', 'vmcreated', 'vmdeleted', 'maxcpu', 'avgcpu', 'p95maxcpu', 'vmcategory', 'vmcorecount', 'vmmemory']
old_vmtable_df = pd.read_csv(vmtable_path, header=None, index_col=False, names=vmtable_headers, delimiter=',')

del old_vmtable_df['subscriptionid']
del old_vmtable_df['deploymentid']
del old_vmtable_df['vmcategory']

vmtable_df = old_vmtable_df.loc[old_vmtable_df['vmcorecount'] == 1]
vmtable_df = vmtable_df.loc[vmtable_df['vmmemory'] == 1.75]
vmtable_df = vmtable_df.loc[vmtable_df['avgcpu'] >= 50]
vmtable_df = vmtable_df.loc[vmtable_df['vmcreated'] == 0]
vmtable_df.to_csv('./vmtables/filtered_vmtables1.csv')

vmtable_df = old_vmtable_df.loc[old_vmtable_df['vmcorecount'] == 1]
vmtable_df = vmtable_df.loc[vmtable_df['vmmemory'] == 0.75]
vmtable_df = vmtable_df.loc[vmtable_df['avgcpu'] >= 50]
vmtable_df = vmtable_df.loc[vmtable_df['vmcreated'] == 0]
vmtable_df.to_csv('./vmtables/filtered_vmtables2.csv')

vmtable_path = './vmtables/filtered_vmtables_50.csv'
vmtable_headers = ['index', 'vmid', 'vmcreated', 'vmdeleted', 'maxcpu', 'avgcpu', 'p95maxcpu', 'vmcorecount', 'vmmemory']
vmtable_df = pd.read_csv(vmtable_path, header=None, index_col=False, names=vmtable_headers, delimiter=',')
del vmtable_df['index']

vm_cpu_df = None
for i in range(1, 2):
    print(i)
    vm_cpu_path = './vm_cpu_readings/vm_cpu_readings-file-%d-of-125.csv' % i
    vm_cpu_headers = ['timestamp', 'vmid', 'mincpu', 'maxcpu', 'avgcpu']
    vm_cpu_df_temp = pd.read_csv(vm_cpu_path, header=None, index_col=False, names=vm_cpu_headers, delimiter=',')
    if vm_cpu_df is None:
        vm_cpu_df = vm_cpu_df_temp.loc[vm_cpu_df_temp['vmid'].isin(vmtable_df['vmid'])]
    else:
        vm_cpu_df_temp = vm_cpu_df_temp.loc[vm_cpu_df_temp['vmid'].isin(vmtable_df['vmid'])]
        vm_cpu_df = pd.concat([vm_cpu_df, vm_cpu_df_temp])
    
#vm_cpu_df = vm_cpu_df.loc[vm_cpu_df['avgcpu'] >= 30]
vm_cpu_df = pd.merge(vm_cpu_df, vmtable_df, how='inner', on=['vmid'])
vmids = vmtable_df['vmid'].values.tolist()
vm_cpu_df = vm_cpu_df.loc[vm_cpu_df['vmid'].isin(vmids)]
vm_cpu_df.to_csv('./test.csv')
'''
test_path = './test.csv'
test_headers = ['index', 'timestamp','vmid','mincpu', 'maxcpu_x', 'avgcpu_x', 'vmcreated', 'vmdeleted', 'maxcpu_y', 'avgcpu_y', 'p95maxcpu','vmcorecount', 'vmmemory']
test_df = pd.read_csv(test_path, header=None, index_col=False, names=test_headers, delimiter=',')

del test_df['vmcreated']
del test_df['vmdeleted']
del test_df['maxcpu_y']
del test_df['avgcpu_y']
del test_df['p95maxcpu']
del test_df['vmcorecount']
del test_df['vmmemory']
del test_df['index']

test_df.to_csv('./vm_result.csv')
vmids = list(set(test_df['vmid'].values.tolist()))
vmids.remove('vmid')
result = {}
for VMs in range(8, 9):
    print(VMs)
    for i in range(0, 8):
        node_name = 'node_%02d' % i
        result[node_name] = {}
        for j in range(0, VMs):
    #            if 'vms' not in result[node_name].keys():
    #                result[node_name]['vms'] = []
            try:
                vmid = vmids.pop()
            except:
                break
            if vmid == 'vmid':
                break
    #            result[node_name]['vms'].append(vmid)
            rows = test_df.loc[(test_df['vmid'] == vmid)]
            for ts in range(0, 70*3):
                timestamp = ts * 300
                if timestamp not in result[node_name].keys():
                    result[node_name][timestamp] = {}
                    result[node_name][timestamp]['maxcpu'] = 0
                    result[node_name][timestamp]['avgcpu'] = 0
                row = rows.loc[(rows['timestamp'] == str(timestamp))]
                try:
                    result[node_name][timestamp]['maxcpu'] = result[node_name][timestamp]['maxcpu'] + float(row['maxcpu_x'].values.tolist()[0])
                    result[node_name][timestamp]['avgcpu'] = result[node_name][timestamp]['avgcpu'] + float(row['avgcpu_x'].values.tolist()[0])
                except:
                    print(rows)
                    break

    with open('./result%d.csv' % (VMs), 'w') as f:
        f.write('node_name, timestamp, avgcpu, maxcpu\n')
        for node_name in result:
            for ts in result[node_name]:
                msg = '%s, %d, %f, %f\n' % (node_name, ts, result[node_name][ts]['avgcpu'], result[node_name][ts]['maxcpu'])
                f.write(msg)
