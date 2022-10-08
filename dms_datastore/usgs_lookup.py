import pandas as pd
import os
import re
import sys
import csv
import math
import shutil

print("\n"*100)
###############################################################################
#   Functions used for processing of string
###############################################################################
# Decompose the identifier string into a list of substrings.
# The splitting occurs at ".", and @. The string that comes after @ is
#   station ID Number, which is removed.
def id_decompose(str_tmp):

    # Substrings to be removed.
    str_rem = [".", "@"]

    # "Cleans up" the string. Use "???" as a temporary place holder.
    for str0 in str_rem:
        str_tmp = str_tmp.replace(str0, "???")

    # Turn string into list.
    str_list = str_tmp.split("???")

    # The last item (station ID) is removed.
    # str_list = str_list[:-1]

    return str_list

# Replace a comma in a string with a semicolon.
# This helps with printing strings into a csv file;
#   if a string contains a comma, the string is unintentionally split.
def comma_to_semicol(str_tmp):

    str_out = str_tmp.replace(",", ";")

    return str_out

#   Converts a string containing spaces into list of substrings
def space_decompose(str_tmp):

    # Substrings to be removed.
    str_rem = [" ", "(", ")"]

    # "Cleans up" the string. Use "???" as a temporary place holder.
    for str0 in str_rem:
        str_tmp = str_tmp.replace(str0, "???")

    # Turn string into list.
    str_list = str_tmp.split("???")

    # Capitalize
    for i in range(0, len(str_list)):
        str_list[i] = str_list[i].upper()

    return str_list

#   Converts a list into a string, separated by "-"
def list_to_string(list_tmp):

    str_out = ""
    count = 1
    for item in list_tmp:
        str_out = str_out + item
        if (count < len(list_tmp)):
            str_out = str_out + "-"
        count = count + 1
    return str_out

###############################################################################
# Main program
###############################################################################
def main():

    ###########################################################################
    # Housekeeping for file I/O
    ###########################################################################

    # USER INPUT: Directory containing the USGS time series.
    fdir = "..\\..\\USGS"

    # USER INPUT: Switch for renaming & copying files
    rename_file = 1

    if (rename_file == 1):
        # Directory in which copies will be placed after renaming.
        destination = os.path.join("D://Hans_Work//Work_Current//USGS_Lookup_with_Eli",
                                   "Renamed")
        if (not(os.path.exists(destination))):
            os.mkdir(destination)

    # Create list of all files (.csv) to be scanned.
    fset = []
    for subdir, dirs, files in os.walk(fdir):
        if (subdir != "."):
            for filename in files:
                filepath = subdir + os.sep + filename
                if (filepath.endswith(".csv")):
                    fset.append(filepath)

    fsetsize = len(fset)

    # Name of the key file read.
    fname_key = "variable_mappings.csv"

    # Name of station mapping file
    fname_stations_utm = "stations_utm.csv"

    # Name of subloc key file
    fname_station_subloc = "station_subloc_new.csv"

    # Name of summary file to be generated.
    fname_result_log = "result_log.csv"

    # Name of mapping file to be generated.
    fname_result_mapping = "result_aquarius_mapping.csv"


    ###########################################################################
    # Initialize lists to store variable information
    ###########################################################################

    # List of variables, and their units, detected from time series.
    list_ts_var = [[] for x in range(0, fsetsize)]
    list_ts_unit = [[] for x in range(0, fsetsize)]

    # List of identifiers, detected from time series.
    list_ts_id = [[] for x in range(0, fsetsize)]

    # List of USGS station IDs, detected from time series.
    list_station_id = [[] for x in range(0, fsetsize)]

    # List of station aliases, read from stations_utm.csv
    #   e.g., sjj for San Joaquin River at Jersey Point
    list_station_alias = [[] for x in range(0, fsetsize)]

    # List of sublocations, detected from time series.
    list_ts_subloc = [[] for x in range(0, fsetsize)]

    # List specifying whether sublocations from timeseries are confirmed.
    list_subloc_confirm = [[] for x in range(0, fsetsize)]

    # List of ACODE (Aquarius Code), from the key file (src_var_id column).
    # Currently (March of 2021), the source variable ID is retained.
    list_acode = [[] for x in range(0, fsetsize)]

    # List of DWR variables, from the key file.
    list_dwr_var = [[] for x in range(0, fsetsize)]

    # List of new file name, following naming convention specified on Confluence.
    list_new_name = [[] for x in range(0, fsetsize)]

    # List of date range, to be used for file name
    list_drange = [[] for x in range(0, fsetsize)]

    ###########################################################################
    # Specify time-aggregation keywords.
    # Timeseries identifiers containing these keywords will be removed.
    # All items need to be capitalized.
    ###########################################################################
    list_time_agg = ["MEAN", "MEDIAN", "MIN", "MAX", "AVE", "AVERAGE", "PEAK",
                     "WY"]

    ###########################################################################
    # Specify keywords that "nullify" list_time_agg
    ###########################################################################
    list_valid = ["CROSS", # "cross section average"
                  ]

    ###########################################################################
    # Specify sublocation keywords.
    ###########################################################################
    list_subloc = ["LOWER", "UPPER", "RIGHT", "LEFT", "BGC"]

    ###########################################################################
    # Loop over the time series files to assign ACODE to each.
    ###########################################################################
    for i in range(0, fsetsize):

        # For monitoring.
        print("Processing %i"%(i+1) + "/" + "%i"%fsetsize)

        # Open time series files in the specified directory.
        filepath = fset[i]
        textfile = open(filepath, 'r')

        # Scan for variable identifier, name, and unit.
        scan_id = re.compile("\# Time-series identifier:\s+(.*)")
        scan_var = re.compile("\# Value parameter:\s+(.*)")
        scan_unit = re.compile("\# Value units:\s+(.*)")
        scan_csv_line = re.compile("\# CSV data starts at line\s+(\d*)")

        found_id = []
        found_var = []
        found_unit = []
        found_csv_line = []

        for line in textfile:
            found_id = found_id + scan_id.findall(line)
            found_var = found_var + scan_var.findall(line)
            found_unit = found_unit + scan_unit.findall(line)
            found_csv_line = found_csv_line + scan_csv_line.findall(line)

            # It's good to have this IF statement since the time series
            #   information is located in the top portion of the file.
            #   Otherwise the scanning takes a very long time.
            if (len(found_id) == 1 and len(found_var) == 1 and
                len(found_unit) == 1 and len(found_csv_line) == 1):
                break

        # Store the found information into repective lists.
        # Decompose the identifer string into a list.
        #   The identifier componetns will be used to match with an ACODE.
        id_ts_raw = found_id[0]
        var_ts = found_var[0]
        unit_ts = found_unit[0]

        id_ts_set = id_decompose(id_ts_raw)

        list_ts_id[i] = id_ts_set[:-1]
        list_station_id[i] = id_ts_set[-1]
        list_ts_var[i] = var_ts
        list_ts_unit[i] = unit_ts

        # One extra step to check for identifier.
        assert len(id_ts_set) <= 7, "Check the matching cases."

        #######################################################################
        # Find date range (used to rename the file)
        #######################################################################
        #df = pd.read_csv(filepath, header = int(found_csv_line[0])-1,
        #                 index_col = 0, low_memory = False)
        #list_drange[i] = [df.index[0][:4], df.index[-1][:4]]

        textfile = open(filepath, 'r')
        lines = textfile.readlines()
        list_drange[i] = [lines[int(found_csv_line[0])][:4],
                          lines[-1][:4]]

        #######################################################################
        # Scan through the key to find a match.
        #######################################################################
        acode = []
        dwr_var = []

        df_key = pd.read_csv(fname_key)
        list_key = df_key["src_var_name"].to_list()
        if (comma_to_semicol(var_ts) in list_key):
            ind_key = list_key.index(comma_to_semicol(var_ts))
            if (df_key["src_name"][ind_key] == "usgs"):
                acode = df_key["src_var_id"][ind_key]
                dwr_var = df_key["var_name"][ind_key]

                # Time aggregation filter: "-999" is assigned if time aggregated
                ita = 0
                for item0 in id_ts_set[2:]:
                    for item1 in space_decompose(item0):

                        # let valid data through (e.g., cross-section ave)
                        if (item1 in list_valid):
                            ita = 1
                            break

                        if (item1 in list_time_agg):
                            acode = -999 # mark for skip
                            ita = 1
                            print("     Time aggregation keyword found:", item1)
                            print("     Variable skipped:", id_ts_raw)
                            break

                    if(ita):
                        break

        # IF ACODE has been assigned, store it to list.
        if acode:
            list_acode[i] = acode
            list_dwr_var[i] = dwr_var

        else:
            print("No match found for:")
            print(comma_to_semicol(var_ts), "|", comma_to_semicol(unit_ts))
            sys.exit()

        # Identify sublocations (if any)
        sublocs = []
        for item0 in id_ts_set[2:]:
            for item1 in space_decompose(item0):

                if (item1 in list_subloc):
                    sublocs.append(item1.lower())

        list_ts_subloc[i] = sublocs

    ###########################################################################
    # Identify files marked to be skipped.
    ###########################################################################
    list_skip = []
    for i in range(0, fsetsize):
        if (int(list_acode[i]) < 0):
            list_skip.append(i)

    ###########################################################################
    # Make sure ACODE is a five-character string value.
    # Sometimes the leading zeros are lost during file handling.
    ###########################################################################
    for i in range(0, fsetsize):
        if (i in list_skip):
            continue
        acode = list_acode[i]

        num_zeros = 5 - len(acode)

        for j in range(0, num_zeros):
            acode = "0" + acode

        list_acode[i] = acode


    ###########################################################################
    # Convert floating NaN to string
    # try-except is used for convenience because of string/float mixed type
    ###########################################################################
    for i in range(fsetsize):
        try:
            if (math.isnan(list_dwr_var[i])):
                list_dwr_var[i] = "UNSPECIFIED"
        except:
            pass

    ###########################################################################
    # Scan through stations_utm to identify station alias
    ###########################################################################
    stations_utm = pd.read_csv(fname_stations_utm, index_col = "id")

    # The "id" column may contain quotation marks. Use list to find location.
    id_list = stations_utm.index.tolist()
    for j in range(0, len(id_list)):
        id_list[j] = id_list[j].replace("'", "")

    for i in range(0, fsetsize):

        if (list_station_id[i] in id_list):
            id_index = id_list.index(list_station_id[i])
            alias = stations_utm.iloc[id_index]["alias"]

            if (type(alias) == str):
                # Remove quotation mark in alias
                alias = alias.replace("'", "")

            else:
                # If "alias" entry is blank, alias is read as nan.
                alias = ""

        else:
            print("Station ID not found in stations_utm.csv")
            sys.exit()

        list_station_alias[i] = alias

    ###########################################################################
    # Confirm that subloc from timeseries file matches subloc from key file.
    ###########################################################################
    stations_subloc = pd.read_csv(fname_station_subloc, header = 3)
    stations_subloc = stations_subloc.set_index("id")

    # The "id" column may contain upper-case letters. Force loewr-case.
    id_list = stations_subloc.index.tolist()
    for j in range(0, len(id_list)):
        id_list[j] = id_list[j].lower()

    for i in range(0, fsetsize):

        sublocs = list_ts_subloc[i]

        # Skip for timeseries with no sublocation info
        if (len(sublocs) == 0):
            list_subloc_confirm[i] = ""
            continue

        for subloc in sublocs:
            if (subloc.lower() in id_list):
                id_index = id_list.index(subloc)
                list_subloc_confirm[i] = stations_subloc.iloc[id_index]["subloc"]

            else:
                list_subloc_confirm[i] = "(not confirmed)"

    ###########################################################################
    # Generate new names
    # Example: USGS station at Jersey point, BGC program gauge
    #          usgs_sjj@bgc_11337190_turbidity_2016_2020.csv
    ###########################################################################
    for i in range(fsetsize):
        if (i in list_skip):
            list_new_name[i] = "-"
            continue

        name_tmp = "usgs" + "_" + \
                            list_station_alias[i] + "@" + \
                            list_to_string(list_ts_subloc[i]) + "_" + \
                            list_station_id[i] + "_" + \
                            list_dwr_var[i] + "_" + \
                            list_drange[i][0] + "_" + \
                            list_drange[i][1] + \
                            ".csv"

        # Force lower-case
        name_tmp = name_tmp.lower()

        # Remove blank space
        list_new_name[i] = name_tmp.replace(" ", "")

    ###########################################################################
    # Place copies in the destination with new names.
    ###########################################################################
    if (rename_file == 1):
        print("Renaming and copying files..")
        for i in range(fsetsize):
            print("     %i"%(i+1) + "/" + "%i"%fsetsize)

            if (i in list_skip):
                continue

            path_current = fset[i]
            path_new = os.path.join(destination, list_new_name[i])
            shutil.copy(path_current, path_new)

    ###########################################################################
    # Generate summary of the match.
    ###########################################################################
    print("Printing summary into file..")

    f = open(fname_result_log, "w")
    f.write("File Name" + ","
            + "ID pt 0" + ","
            + "ID pt 1" + ","
            + "ID pt 2" + ","
            + "ID pt 3" + ","
            + "ID pt 4" + ","
            + "ID pt 5" + ","
            + "Value Parameter" + ","
            + "Value Units" + ","
            + "DWR Variable Name" + ","
            + "ACODE" + ","
            + "Station ID" + ","
            + "Station Alias" + ","
            + "Sublocation" + ","
            + "Sublocation Confirmed" + ","
            + "New Filename" + "\n")

    for i in range(0, fsetsize):
        fname = os.path.basename(fset[i])
        f.write(comma_to_semicol(fname) + ",")

        # Print identifier components.
        #   Try-except is used because number of components varies.
        list0 = list_ts_id[i]
        for j in range(0,6):
            try:
                f.write(comma_to_semicol(list0[j]) + ",")
            except IndexError:
                f.write(",")

        # Print value parameter and unit.
        f.write(comma_to_semicol(list_ts_var[i]) + ","
                + comma_to_semicol(list_ts_unit[i]) + "," )

        # Print DWR variable name
        f.write(list_dwr_var[i] + ",")

        # Print ACODE. Indicate if variable is skipped.
        if (i in list_skip):
            f.write("SKIPPED,")

        else:
            acode = list_acode[i]
            f.write(acode + ",")

        # Print station ID
        f.write(list_station_id[i] + ",")

        # Print station alias
        f.write(list_station_alias[i] + ",")

        # Print sublocation
        f.write(list_to_string(list_ts_subloc[i]) + ",")

        # Print confirmation of sublocation
        f.write(list_subloc_confirm[i] + ",")

        # Print New Filename
        f.write(list_new_name[i] + ",")

        f.write("\n")

    f.close()

    ###########################################################################
    # Generate mapping
    ###########################################################################
    print("Printing mapping..")
    mapping_acode = []
    mapping_ts_var = []
    mapping_ts_unit = []
    mapping_dwr_var = []
    for i in range(0, fsetsize):

        if(i in list_skip):
            continue

        acode = list_acode[i]

        if(acode in mapping_acode):
            continue
        else:
            mapping_acode.append(acode)
            mapping_ts_var.append(list_ts_var[i])
            mapping_ts_unit.append(list_ts_unit[i])
            mapping_dwr_var.append(list_dwr_var[i])

    f = open(fname_result_mapping, "w")
    f.write("Value Parameter" + ","
            + "Value Units" + ","
            + "Aquarius Code" + ","
            + "DWR Variable Name \n")

    for k in range(0, len(mapping_acode)):
        f.write(comma_to_semicol(mapping_ts_var[k]) + ","
                + comma_to_semicol(mapping_ts_unit[k]) + ","
                + mapping_acode[k] + ","
                + mapping_dwr_var[k] + "\n")

    f.close()

    print("Done.")

if __name__ == '__main__':
    main()