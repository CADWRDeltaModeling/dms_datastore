#!/usr/bin/env python
# -*- coding: utf-8 -*-import pandas as pd
import glob
import os
import sys
import matplotlib.pyplot as plt
from vtools.datastore.read_ts import *
import shutil

def almost_match(x,y):
    xmatch = os.path.split(x)[1]
    xmatch = os.path.splitext(xmatch)[0]
    ymatch = os.path.split(y)[1]
    ymatch = os.path.splitext(ymatch)[0]
    if len(xmatch) < 9 or len(ymatch) < 9: return False
    return xmatch[:-4] == ymatch[:-4]


def compare_dir(base,comp,apply_change=False):
    print(f"Computing matches between base={base} and comp={comp}") 
    base_files = glob.glob(os.path.join(base,"*"))
    base_files = [os.path.split(x)[1] for x in base_files if os.path.isfile(x)]
    comp_files = set(glob.glob(os.path.join(comp,"*")))
    comp_files = set([os.path.split(y)[1] for y in comp_files if os.path.isfile(y)])
    comp_files2 = comp_files.copy()
    
    comp_matched = set()
    base_matched = set()
    base_not_matched = set()
    almost = {}
    
    for b in base_files:
        if b in comp_files: 
            base_matched.add(b)
            comp_matched.add(b)
        else:
            for c in comp_files:
                if almost_match(b,c):
                    almost[b] = c
                    break
            if not b in almost:        
                base_not_matched.add(b)
    
    nmatch = len(base_matched)    
    
    print(f"\nExact base matches: {nmatch}")
    if apply_change:
        for item in base_matched:   # move from comp to base
            shutil.copy(os.path.join(comp,item),os.path.join(base,item))
    
    nnear = len(almost)
    print(f"\nNear matches except for final year, written as base: comp (total {nnear})")
    keys = list(almost.keys())
    keys.sort()
    for item in almost.keys():
        print(f"{item}: {almost[item]}")
        if apply_change:
            os.remove(os.path.join(base,item))  # get rid of the original in base
            # copy in the almost matching one from comp
            shutil.copy(os.path.join(comp, almost[item]), os.path.join(base,almost[item]))    
    
    # take all comp_files and removed ones that have been matched or almost matched
    for item in comp_matched:
        comp_files.remove(item)   # removing items from set, not files here
    for item in almost.values():   # remove almost match
        if item in comp_files:
            comp_files.remove(item)    
        
    nunbase = len(base_not_matched)
    base_not_matched = list(base_not_matched)
    base_not_matched.sort()
    
    
    
    print(f"\nUnmatched files in base dir (total {nunbase}):")
    for item in base_not_matched:
        print(item)
        if apply_change:
            os.remove(os.path.join(base,item))

        
    nuncomp = len(comp_files)
    comp_files = list(comp_files)
    comp_files.sort()
    print(f"\nUnmatched files in comp dir (total {nuncomp})")
    for item in comp_files:
        print(item)
        if apply_change:
            shutil.copy(os.path.join(comp, item), os.path.join(base,item))

def main():
    apply_change = "--apply_change" in sys.argv
    base_comp = [x for x in sys.argv[1:] if x.find("--apply_change")<0]
    if len(base_comp) != 2: print("Usage: compare_directories base comp [--apply_change]")
    if apply_change: 
        print("--apply_change selected")
    else:
        print("--apply_change not selected")
    compare_dir(base=base_comp[0],comp=base_comp[1],apply_change=apply_change)    

if __name__ == '__main__':
    main()