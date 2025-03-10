#!/usr/bin/env python

"""conf.py  :  Configuration file """

####---- Configuration file to operate through BioMongo ----####

# ----------
# General information
# ----------
operation='' # Operations: insert, update_one, update_all, update_with_file, upsert_one, upsert_all, upsert_with_file, restore_one, restore_all, add_empty_field, add_field_with_file, rename_field, remove_field
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
# Update and upsert operations need (update_field is always needed):
# ----------
update_field='' # Target field to be updated.
new_value='' # New value for the field (no need if using a file).
update_criteria={'':''} # Criteria to update one, pick a field with unique values.
# If using update_with_file or upsert_with_file, please provide the csv with the information.
update_file = '' # If you want to add a list as a new value, separate the values with ";". You can link to a directory full of csv files, too.

# ----------
# Restore needs:
# ----------
restore_criteria={'field_to_match':'value_to_match'} # Criteria to restore one file.
log_id='' # Log id to the version to be restored.

# ---------
# Add new empty field needs:
# ----------
new_field='' # Name of the new field to be added. Please do not use empty spaces or special characters

# ---------
# Add new field with file needs:
# ----------
new_field_file='/path/to/csv' # If using add_field_with_file, please provide the csv with the information. The structure should be the same as the update file

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
