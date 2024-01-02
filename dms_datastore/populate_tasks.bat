
echo started > log.txt

set repo=\\cnrastore-bdo\Modeling_Data\repo\continuous
set repo_staging=\\cnrastore-bdo\Modeling_Data\repo_staging\continuous
echo activate >> log.txt
call c:\Users\eli\miniconda3\Scripts\activate.bat schism4
if %errorlevel% neq 0 exit /b %errorlevel%


echo deleting >> log.txt
del /Q raw\* formatted\*
del compare_raw.txt compare_formatted.txt 
echo populate
echo populate >> log.txt
call populate_repo --dest=raw > populate_log.txt
echo delete
echo delete >> log.txt
rem call delete_from_filelist --dpath=raw 
echo reformat
echo reformat >> log.txt
call reformat --inpath raw --outpath formatted >> populate_log.txt

echo process usgs
echo splitting or consolidating usgs multi column files >> log.txt
usgs_multi --fpath formatted  >> populate_log.txt
echo creating inventories
echo inventory >> log.txt
call inventory --repo formatted

rem At this point format gets checked
call compare_directories --base %repo%/raw --compare %repo_staging%/raw > compare_raw.txt

call compare_directories --base %repo%/formatted --compare %repo_staging%/formatted > compare_formatted.txt
call compare_directories --base %repo%/screened --compare %repo_staging%/screened > compare_screened.txt


echo done
echo done >> log.txt