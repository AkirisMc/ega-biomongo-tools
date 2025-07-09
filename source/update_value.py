#!/usr/bin/env python

"""update_value.py  :  Update embedded or non-embedded fields in a collection, even if the fields are not already present in the documents """

__author__ = "Akiris Moctezuma"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

from . import log_functions
import pandas as pd
import numpy as np
import os
from pymongo import UpdateOne
from os import listdir
from os.path import isfile, join, isdir
import pandas as pd
import re

def updateOne(operation, db, collection_name, update_criteria, update_field, new_value, name, method):
    """
    Update the value of an embedded or non-embedded field in one document present in a specific collection from the database.
    If the field doesn't exist, the program forcefully creates it at the specified location and adds the given value.
    """
    # Access the collection:
    collection = db[collection_name]
    
    # Find the document before the update to retrieve the previous value
    previous_document = collection.find_one(update_criteria)
    
    if previous_document:
        updates_made = 0

        # Ensure new_value is processed correctly
        if isinstance(new_value, str):  
            lowered_value = new_value.lower().strip()  # Normalize case and remove spaces

            if lowered_value in ["true", "false"]:  # Convert boolean-like strings
                new_value_list = lowered_value == "true"  
            elif lowered_value == "none":  # Convert "None" string to Python None
                new_value_list = None
            elif ";" in new_value:  # Convert semicolon-separated strings into lists
                new_value_list = new_value.split(";")
            else:
                new_value_list = new_value  # Keep as-is for other strings
        else:
            new_value_list = new_value  

        # Insert metadata about the update process in the log_details collection
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)

        # Retrieve the current value using dot notation (for embedded fields)
        current_value = previous_document
        for key in update_field.split("."):
            current_value = current_value.get(key)
            if current_value is None:
                break
               
        # If the field doesn't exist in the document, explicitly set `None` as the current value
        if current_value is None:
            print(f"Field {update_field} doesn't exist or has no value in the document. Creating field and setting new value.")
            updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, None, new_value_list)
        elif current_value != new_value_list:
            print(f"Field {update_field} exists and has a different value in document with stable_id: {list(update_criteria.values())[0]}. Updating the field.")
            updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, current_value, new_value_list)
        else:
            print(f"Field {update_field} exists but has the same value in document with stable_id: {list(update_criteria.values())[0]}. No update required.")
            return  # Exit the function without performing the update if values are the same

        # Update the document with the new data
        result = collection.update_one(update_criteria, {"$set": {update_field: new_value_list, "log": updated_log}})
            
        # Print whether the document was updated or not
        if result.modified_count > 0:
            updates_made += 1
            print(f'Field {update_field} updated successfully in the document with stable_id: {list(update_criteria.values())[0]}')
            print(f'Previous value: {current_value}, New value: {new_value_list}')
            print('')
        elif updates_made == 0:
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
    else:
        print(f"The document you are searching for is not in the collection.")

def updateAll(operation, db, collection_name, update_field, new_value, name, method):
    """
    Update the value of an embedded or non-embedded field in all the documents present in a specific collection from the database.
    If the field doesn't exist, the program forcefully creates it at the specified location and adds the given value.
    """
    # Access the collection
    collection = db[collection_name]

    # Prepare a list of bulk update operations
    bulk_updates = []

    # Fetch all documents in the collection
    previous_documents = collection.find()

    # Insert metadata about the update process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Ensure new_value is processed correctly
    if isinstance(new_value, str):
        lowered_value = new_value.lower().strip()  # Normalize case and remove spaces

        if lowered_value in ["true", "false"]: # Convert boolean-like strings  
            new_value_list = lowered_value == "true"  
        elif lowered_value == "none": # Convert "None" string to Python None  
            new_value_list = None   
        elif ";" in new_value:  # Convert semicolon-separated strings into lists
            new_value_list = new_value.split(";")
        else:
            new_value_list = new_value  # Keep as-is for other strings
    else:
        new_value_list = new_value    

    # Loop through each document
    for document in previous_documents:
        stable_id = document.get('stable_id', 'Unknown stable_id')

        # Retrieve the current value using dot notation 
        current_value = document
        for key in update_field.split("."):
            current_value = current_value.get(key)
            if current_value is None:
                break

        if current_value is None:
            # If the field is set as Null or doesn't exist, create it and set the new value
            print(f"Field {update_field} doesn't exist or has no value in document with stable_id: {stable_id}. Creating field and setting new value.")
            updated_log = log_functions.updateLog(document, process_id, operation, update_field, None, new_value_list)
            bulk_updates.append(UpdateOne(
                {"_id": document["_id"]},
                {"$set": {update_field: new_value_list, "log": updated_log}}
            ))
        elif current_value != new_value_list:
            # If the field exists but the value is different, update it
            print(f"Field {update_field} exists and has a different value in document with stable_id: {stable_id}. Updating the field.")
            updated_log = log_functions.updateLog(document, process_id, operation, update_field, current_value, new_value_list)
            bulk_updates.append(UpdateOne(
                {"_id": document["_id"]},
                {"$set": {update_field: new_value_list, "log": updated_log}}
            ))
        else:
            # If the field exists and the value is the same, no update is needed
            print(f"Field {update_field} exists but has the same value in document with stable_id: {stable_id}. No update required.")

    # Execute the bulk update operations
    if bulk_updates:
        result = collection.bulk_write(bulk_updates)
        updates_made = result.modified_count
        if updates_made == 0:
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
        else:
            print(f'{updates_made} document(s) updated successfully. New value for {update_field}: {new_value_list}')
    else:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were necessary. All values were already up to date.")

def updateFile(operation, db, collection_name, update_file, name, method):
    """
    Update the value of an embedded or non-embedded field in multiple documents with information from a CSV file.
    If the field doesn't exist, the program forcefully creates it at the specified location and adds the given value.
    Supports a single CSV file or multiple files in a directory.
    """

    # Determine if it's a single file or a directory
    if os.path.exists(update_file) and (isfile(update_file) or isdir(update_file)):
        # Obtain list of elements found in the directory, turn them into iterable lists
        if isfile(update_file):
            csv_files = [update_file]
            print("There is 1 file to process.")
        elif isdir(update_file):
            csv_files = [update_file + "/" + f for f in listdir(update_file) if isfile(join(update_file, f))]
            csv_files = sorted(csv_files, key=lambda s: [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)])
            print(f'There are {len(csv_files)} files to process.')

        # Begin loop
        for f in csv_files:
            if f.endswith(".csv"):
                print(f'Importing {f}')
                update_data = pd.read_csv(f)

                # Extract column headers
                column_names = update_data.columns.to_list()
                field_to_match = column_names[0]  # The header from the first column will always be the field to match
                update_field = column_names[1]  # The header from the second column will always be the update field

                # Get the values from the columns
                values_to_match = update_data[field_to_match].values # The stable_id's
                new_values = update_data[update_field].values # The new values

                print(f'There are {len(values_to_match)} objects to update.')

                # Access the collection:
                collection = db[collection_name]

                # Begin processing updates
                print(f"Processing updates...")

                # Insert metadata about the update process
                process_id = log_functions.insertLog(db, name, method, operation, collection_name)

                # Track the documents that were actually updated
                updates_made = 0

                for value_to_match, new_value in zip(values_to_match, new_values):
                    # Ensure new_value is processed correctly
                    if pd.isna(new_value) or new_value is None:
                        new_value_list = None # if value is absent or None
                    elif isinstance(new_value, np.bool_):
                        new_value_list = bool(new_value) # if a value is a numpy bool (boolean)
                    else:
                        new_value_list = new_value.split(";") if isinstance(new_value, str) and ";" in new_value else new_value  # if a value is a single string element or several string elements (list)
                        
                    # Stable id of the object to be updated
                    update_criteria = {field_to_match: value_to_match}

                    # Find the document before the update to retrieve the previous value
                    previous_document = collection.find_one(update_criteria)

                    if previous_document:
                        # Retrieve the current value using dot notation 
                        current_value = previous_document
                        for key in update_field.split("."):
                            current_value = current_value.get(key)
                            if current_value is None:
                                break

                        if current_value is None:
                            # If the field is set as Null or doesn't exist, create it and set the new value
                            print(f"Field '{update_field}' doesn't exist or has no value in document with stable_id: {value_to_match}. Creating field and setting new value.")
                            updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, None, new_value_list)
                            result = collection.update_one(update_criteria, {"$set": {update_field: new_value_list, "log": updated_log}})
                            if result.modified_count > 0:
                                updates_made += 1
                        elif current_value != new_value_list:
                            print(f"Field '{update_field}' already exists and has a different value in document with stable_id: {value_to_match}. Updating the field.")
                            updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, current_value, new_value_list)
                            result = collection.update_one(update_criteria, {"$set": {update_field: new_value_list, "log": updated_log}})
                            if result.modified_count > 0:
                                updates_made += 1
                        else:
                            print(f"Field '{update_field}' already exists and has the same value in document with stable_id: {value_to_match}. No update required.")
                    else:
                        print(f"The document with {field_to_match} {value_to_match} is not in the collection. Creating new document, new field and setting new value.")
                        
                        # Initialize new document with the matching field
                        temp_document = {field_to_match: value_to_match}

                        # Create nested structure for the update_field
                        keys = update_field.split(".")
                        nested = temp_document
                        for key in keys[:-1]:
                            nested[key] = {}
                            nested = nested[key]
                        nested[keys[-1]] = new_value_list

                        collection.insert_one(temp_document)
                        updates_made += 1

                        # Insert log for the new document
                        updated_log = log_functions.updateLog(temp_document, process_id, operation, update_field, None, new_value_list)
                        result = collection.update_one({field_to_match: value_to_match},{"$set": {"log": updated_log}})

                # Log the results of updates
                if updates_made > 0:
                    print(f"Total number of updates made: {updates_made}.")
                else:
                    log_functions.deleteLog(db, str(process_id))
                    print(f"No changes were made.")    
            else:
                print(f"{f} is not a CSV file.")
        print("Updates finished!")
    else:
        print(f"{update_file} file or directory does not exist.")