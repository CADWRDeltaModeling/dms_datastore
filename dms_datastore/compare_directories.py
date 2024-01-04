#!/usr/bin/env python
# -*- coding: utf-8 -*-import pandas as pd
import glob
import re
import os
import sys
import pandas as pd
import argparse
import datetime as dtm
import matplotlib.pyplot as plt
from dms_datastore import dstore_config
from dms_datastore.read_ts import *
import shutil

__all__ = ["compare_dir"]


def almost_match(x,y):
    xmatch = os.path.split(x)[1]
    xmatch = os.path.splitext(xmatch)[0]
    ymatch = os.path.split(y)[1]
    ymatch = os.path.splitext(ymatch)[0]
    if len(xmatch) < 9 or len(ymatch) < 9: return False
    return xmatch[:-4] == ymatch[:-4]


def _trim_using_exceptions(fnamelist, exceptions, base_present, compare_present):
    """ Given a list of file names and exceptions, remove if the exceptions match
    
    Parameters
    ----------
    fnamelist : list
        The list to filter
    
    exceptions : pd.DataFrame 
        DataFrame with at least the columns 'file_pattern','base','compare'
    file_pattern should be a regular expression. The expression {current_year} is
    replaced, but consider whether this is what you want. It is great for a view
    but probably not what you want when you are applying updates/changes because only un-excepted
    items are updated
        
    base_present : bool
        The context (file present in base/compare) in which the exception 
        will be used. For instance if base_compare_present is (True,False) 
        the exception will be applied in cases where the base directory
        contains data and the compare directory dies not.
    
    compare_present : bool
        The context (file present in base/compare) in which the exception 
        will be used. For instance if base_compare_present is (True,False) 
        the exception will be applied in cases where the base directory
        contains data and the compare directory dies not.
    
    Returns
    -------
    fnamelist : list(str)
        Reduced list
        
    Notes
    -----
    Here is an example exceptions file. It has a {current_year} template. Note the limitations
    above
    
    # Exceptions file example
    file_pattern,base,compare
    noaa.*predictions_.*.csv,True,False
    cdec_clc_clc_ph_19.*.csv,True,False
    .*salinity.*.csv,True,False
    .*velocity.*.csv,True,False
    .*_{current_year}.csv,False,True
    
    """

    if exceptions is None: 
        return fnamelist

    current_year = dtm.date.today().year        
    fnamelistfilt = fnamelist.copy()
    userows = (exceptions["compare"]==compare_present) & (exceptions["base"]==base_present)
    exceptions_sub = exceptions[userows]
    for ndx,row in exceptions_sub.iterrows():
        patstr = row.file_pattern
        patstr = patstr.replace("{current_year}",str(current_year)) #format({"current_year": current_year})
        pat = re.compile(patstr)
        fnamelistfilt = [ f for f in fnamelistfilt if not pat.match(f) ] 
    return fnamelistfilt
    

def compare_dir(base,
                comp,
                pat="*",
                exceptions=None,
                apply_change=False,
                apply_update=False,
                year2=None,
                outfile=None):


    if isinstance(exceptions,str):
        exceptions = load_exceptions(exceptions)
        
    if outfile is not None:
        out = open(outfile,"w")
    else: 
        out = None
    
    def outline(line):
        print(line) 
        if out is not None:
            out.write(line+"\n")
            
    outline(f"Computing matches between base={base} and comp={comp}") 
    if not(os.path.exists(base)):
        raise ValueError("Base directory does not exist")
    if not(os.path.exists(comp)):
        raise ValueError("Comparison directory does not exist")       
    
    
    base_files = glob.glob(os.path.join(base,pat))
    base_files = [os.path.split(x)[1] for x in base_files if os.path.isfile(x)]
    comp_files = set(glob.glob(os.path.join(comp,pat)))
    comp_files = set([os.path.split(y)[1] for y in comp_files if os.path.isfile(y)])
    comp_files2 = comp_files.copy()
    
    comp_matched = set()
    base_matched = set()
    base_not_matched = set()
    almost = {}

    twoyears = re.compile(r".*_[1-2][901][0-9]{2}_[1-2][901][0-9]{2}\.csv")    
    if year2 is None:
        year2 = all([twoyears.match(fname) for fname in base_files])

    
    for b in base_files:
        if b in comp_files: 
            base_matched.add(b)
            comp_matched.add(b)
        elif year2:
            for c in comp_files:
                if almost_match(b,c):
                    almost[b] = c
                    break
            if not b in almost:        
                base_not_matched.add(b)
        else:  # partial matches up to final year of two year pattern not considered
            base_not_matched.add(b)     
    
    nmatch = len(base_matched)    
    
    outline(f"\nExact base matches: {nmatch}")
    # files that have changed but name is present in both
    if apply_change or apply_update:
        for item in base_matched:   # move from comp to base
            shutil.copy(os.path.join(comp,item),os.path.join(base,item))

    if year2: 
        # files whose name is present in both except for the final year of 
        # a two part year suffix like _2007_2021 and _2007_2022
        nnear = len(almost)
        outline(f"\nNear matches except for final year, written as base: comp (total {nnear})")
        keys = list(almost.keys())
        keys.sort()
        for item in almost.keys():
            outline(f"{item}: {almost[item]}")
            if apply_change or apply_update:
                os.remove(os.path.join(base,item))  # get rid of the original in base
                # copy in the almost matching one from comp
                shutil.copy(os.path.join(comp, almost[item]), os.path.join(base,almost[item]))    
    else:
        almost = {}
        nnear = 0
    
    # take all comp_files and removed ones that have been matched or almost matched
    # from the lists so it doesn't get treated twice
    for item in comp_matched:
        comp_files.remove(item)   # removing items from set, not files here
    for item in almost.values():   # remove almost match
        if item in comp_files:
            comp_files.remove(item)    
        
    nunbase = len(base_not_matched)
    base_not_matched = list(base_not_matched)
    pretrimlen_bnm = len(base_not_matched)
    base_not_matched = _trim_using_exceptions(base_not_matched,exceptions,True,False)
    base_not_matched.sort()
    posttrimlen_bnm = len(base_not_matched)
    ntrim_bnm = pretrimlen_bnm - posttrimlen_bnm
    
    outline(f"\nUnmatched files in base dir (total {nunbase}, {ntrim_bnm} exceptions, net {nunbase-ntrim_bnm} listed):")
    for item in base_not_matched:
        outline(item)
        if apply_change:
            # this would be apply_change, which mirrors the 
            # compare dir. This can be helpful, but approach with caution
            # apply_update doesn't do this
            os.remove(os.path.join(base,item))
        
    nuncomp = len(comp_files)
    comp_files = list(comp_files)
    pretrimlen_cf = len(comp_files)
    comp_files = _trim_using_exceptions(comp_files,exceptions,False,True)
    postrimlen_cf = len(comp_files)
    ntrim_cf = pretrimlen_cf - postrimlen_cf
    comp_files.sort()
    
    
    outline(f"\nUnmatched files in comp dir (total {nuncomp}, {ntrim_cf} exceptions, net {nuncomp-ntrim_cf} listed):")
    for item in comp_files:
        outline(item)
        if apply_change or apply_update:
            shutil.copy(os.path.join(comp, item), os.path.join(base,item))

    if exceptions is not None:
        out.write("\n\n******** Exceptions ******\n\n")
        exceptions.to_csv(out,sep=",",index=None,lineterminator="\n")

    if out is not None:
        out.close()

def create_arg_parser():
    parser = argparse.ArgumentParser('Compare contents of two repositories')

    parser.add_argument('--pat', default='*',
                        help='Pattern of files to compare, default is *.')
    parser.add_argument('--apply_change',action="store_true", 
                        help='Apply all changes detected.')
    parser.add_argument('--apply_update',action="store_true", 
                        help='Copy new and changed files but not deletions.')
    parser.add_argument('--excepts',
                        default=None,
                        help="""Optional name of csv file storing exceptions. 
                              Can also be a config key that points to such a file
                              CSV should have headers 
                              'file_pattern','base','compare'. 
                              First column is a regular expression, with {current_year}
                              available as a substitution that will be filled with year (this is sometimes
                              an advantage for thinning differences but may not be useful when --apply_update
                              is selected, because the more recent files won't be moved. The next two
                              base and compare are boolean columns that 
                              collectively describe the situation 
                              (file present in each) 
                              in which exception is made""")                        
    parser.add_argument('--year2',default=None,type=bool, 
                        help="""Match if years that are the 
                        same up to the second year 
                        in files with start and end year format. 
                        For example name_2020_2023.csv 
                        would be identified as an update to name_2020_2022.csv""")
    parser.add_argument('--base',
                        type=str,
                        help='Base (existing) directory')
    parser.add_argument('--compare',type=str,help='Comparison directory') 
    parser.add_argument('--outfile',type=str,help='Comparison output file path')     
    return parser


def load_exceptions(excepts):
    if excepts is None:
        return excepts
    elif type(excepts) == pd.DataFrame:
        df = excepts
    elif os.path.exists(excepts):
        print(f"loading exceptions from {excepts}")
        df = pd.read_csv(excepts,
                         header=0,
                         sep=",",
                         index_col=None,
                         comment="#",
                         dtype={"file_pattern":str,"base":bool,"compare":bool})
    else:
        path = dstore_config.config_file(excepts)
        print(f"loading exceptions from {path}")
        df = pd.read_csv(path,
                         header=0,
                         sep=",",
                         index_col=None,
                         comment="#",
                         dtype={"file_pattern":str,"base":bool,"compare":bool})
    return df

def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    pat = args.pat
    apply_change = args.apply_change
    apply_update = args.apply_update    
    base = args.base
    compare = args.compare
    year2 = args.year2
    excepts = args.excepts
    exceptions = load_exceptions(excepts)
    outfile = args.outfile    
    
    
    if apply_change: 
        print("--apply_change selected")
    else:
        print("--apply_change not selected")
    if apply_update: 
        print("--apply_update selected")
    else:
        print("--apply_update not selected")
    if apply_change and apply_update:
        raise("apply_change and apply_update are mutually exclusive")    
        
    compare_dir(base=base,comp=compare,pat=pat,exceptions=exceptions,
                apply_change=apply_change,
                apply_update=apply_update,
                year2=year2,
                outfile=outfile)    

if __name__ == '__main__':
    main()