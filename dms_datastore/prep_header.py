#!/usr/bin/env python
# -*- coding: utf-8 -*-
import yaml



def block_comment(txt):
    text = txt.split("\n")
    text =  ["# "+x for x in text]
    return "\n".join(text)

def prep_header(metadata,format_version):
    """ Prepares metadata in the form of a string or yaml data structure for inclusion
        Prep includes making sure that the lines are commented and start with the format: line
    """
    if isinstance(metadata,str):
        metadata = metadata.split("\n")
        if not "format" in metadata[0]:
            if metadata[0].startswith("#"):
                metadata = [f"# format : {format_version}"]+metadata  
            else:
                metadata = [f"format : {format_version}"]+metadata
            # Get rid of conflicting line
            conflict = -1
            for i in range(1,len(metadata)):
                if "format" in metadata[i]: conflict = i
            if conflict > 0: 
                del(metadata[conflict])
        if not metadata[0].startswith("#"): 
            metadata = ["# "+x for x in metadata]
        header = "\n".join(metadata)
    else: # yaml
        if "format" in metadata:
            del(metadata["format"])
        header_no_comment = yaml.dump(metadata)
        header = block_comment(header_no_comment)
        if not "format" in metadata:
            header = "# format: dwr-dms-1.0\n" + header
    if not header.endswith("\n"): header=header+"\n"
    return header
        

def test_block_comment():
    teststr = \
    """
    This is
    a test"""
    print(block_comment(teststr))

def test_prep_header():
    header = {"format": "something else","unit":"feet"}
    print(prep_header(header,format_version="dwr-dms-1.0"))

    header = {"apple":"orange","unit":"feet","format": "something else"}
    print(prep_header(header,format_version="dwr-dms-1.0"))

    header = {"apple":"orange","unit":"feet"}
    print(prep_header(header,format_version="dwr-dms-1.0"))


    header = "# format: dwr-dms-1.0\n# unit : feet"
    print(prep_header(header,format_version="dwr-dms-1.0"))    

    header = "# unit : feet\n# format: dwr-dms-1.0\n"
    print(" ")
    print(prep_header(header,format_version="dwr-dms-1.0"))    

if __name__ == "__main__":
    test_prep_header()