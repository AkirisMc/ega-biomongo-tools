#!/usr/bin/env python

"""conf.py  :  Configuration file """

####---- Configuration file to operate through BioMongo ----####

# ----------
# General information
# ----------
operation='' # Operations: insert, update_one, update_all, update_with_file, restore_one, restore_all, add_empty_field, rename_field, remove_field
name='' # Name of the person that does this operation.
method='' # Method used to obtain or modify the data (e.g. Raw data EGAPRO).
database_name='' # Name of the database.
collection_name='' # Collection to be managed (analysis, dac, dataset, experiment, file, policy, run, sample, study).


# Depending on the operation you should include the relevant information.

# ---------
# Insert needs:
# ----------
json_documents=f'' # Path to a json document or directory to be inserted.

# ----------
# Update operations need: 
# ----------
update_field='' # Target field to be updated (for update_one and update_all).
new_value='' # New value for the field (for update_one and update_all).
update_criteria={'stable_id':''} # Criteria for update_one, the first element should be the field name to match (stable_id), and the second should be the actual stable_id value.
# If using update_with_file please provide the path of the CSV/JSON with the information or the path of the directory with the CSVs/JSONs.  
update_file=''
# Important to consider
## If you want to add a list as a new value, separate the values with ";".
## If you want to modify an embedded/nested field, use dot notation (e.g. archived_at.crg)  

# ----------
# Restore needs:
# ----------
restore_field='' # Target field to be restored (for restore_one and restore_all).
log_id='' # log_id to the version to be restored (for restore_one and restore_all).
restore_criteria={'stable_id':''} # Criteria for restore_one, the first element should be the field name to match (stable_id), and the second should be the actual stable_id value. 


# ---------
# Add empty field needs:
# ----------
new_field='' # Name of the new field to be added. Please do not use empty spaces or special characters

# ----------
# Rename needs:
# ----------
field_name='' # Name of the field to be changed.
new_field_name='' # New name for the above stated field

# ---------
# Remove needs:
# ---------
# Take into account that you will remove the information from the field in all the files in the colection.
field_to_remove='' # Name of the field to be removed.
