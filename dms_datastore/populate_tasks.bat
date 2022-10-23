

echo populate
populate_repo --dest=raw
echo delete 
delete_from_filelist dpath=raw 
echo reformat
reformat --inpath raw --outpath formatted