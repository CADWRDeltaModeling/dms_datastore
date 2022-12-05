
echo started > log.txt

echo activate >> log.txt
call c:\Users\eli\miniconda3\Scripts\activate.bat py39

echo deleting >> log.txt
del /Q raw\* formatted\*
echo populate
echo populate >> log.txt
call populate_repo --dest=raw
echo delete
echo delete >> log.txt
rem call delete_from_filelist --dpath=raw 
echo reformat
echo reformat >> log.txt
call reformat --inpath raw --outpath formatted

echo creating inventories
echo inventory >> log.txt
call inventory --repo formatted

call compare_directories --base w:/continuous_station_repo_beta/raw --compare ./raw > compare_raw.txt

call compare_directories --base w:/continuous_station_repo_beta/formatted_1yr --compare ./formatted > compare_formatted.txt



echo done
echo done >> log.txt