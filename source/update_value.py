#!/usr/bin/env python

"""update_value.py  :  Update embedded or non-embedded fields in a collection, even if the fields are not already present in the documents """

__author__ = "Akiris Moctezuma"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

import json
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
    Update the value of an embedded or non-embedded field in multiple documents with information from a CSV or JSON file.
    If the field doesn't exist, the program forcefully creates it at the specified location and adds the given value.
    Supports a single CSV/JSON file or multiple files in a directory.
    """
    # Initialize lists to hold CSV and JSON files
    csv_files = []
    json_files = []

    # Access the collection
    collection = db[collection_name]

    # Check if the update_file is a valid file or directory
    if os.path.exists(update_file) and (isfile(update_file) or isdir(update_file)):

        # Single file
        if isfile(update_file):
            if update_file.endswith(".csv"):
                csv_files = [update_file]
                print("There is 1 CSV file to process.")
            elif update_file.endswith(".json"):
                json_files = [update_file]
                print("There is 1 JSON file to process.")
            else:
                print("The file is neither a CSV nor a JSON.")
                return

        # Directory
        elif isdir(update_file):
            files = [f for f in listdir(update_file) if isfile(join(update_file, f))]
            csv_files = [join(update_file, f) for f in files if f.endswith(".csv")]
            json_files = [join(update_file, f) for f in files if f.endswith(".json")]

            if not csv_files and not json_files:
                print(f"{update_file} directory does not contain any CSV or JSON files.")
                return

            if csv_files:
                csv_files = sorted(csv_files, key=lambda s: [int(t) if t.isdigit() else t.lower() for t in re.split('([0-9]+)', s)])
                print(f"There are {len(csv_files)} CSV file(s) to process.")

            if json_files:
                json_files = sorted(json_files, key=lambda s: [int(t) if t.isdigit() else t.lower() for t in re.split('([0-9]+)', s)])
                print(f"There are {len(json_files)} JSON file(s) to process.")

        ### Process CSV files ###
        for csv_file in csv_files:
            print(f"Processing {csv_file}")
            update_data = pd.read_csv(csv_file)

            # Ensure the CSV has at least two columns: one for matching and one for the field to update
            column_names = update_data.columns.to_list()
            field_to_match = column_names[0]
            update_field = column_names[1]
            values_to_match = update_data[field_to_match].values
            new_values = update_data[update_field].values

            # Insert metadata about the update process in the log_details collection
            process_id = log_functions.insertLog(db, name, method, operation, collection_name)

            updates_made_csv = 0

            # Loop through each value in the CSV
            for value_to_match, new_value in zip(values_to_match, new_values):
                if pd.isna(new_value) or new_value is None:
                    new_value_list = None
                elif isinstance(new_value, np.bool_):
                    new_value_list = bool(new_value)
                else:
                    new_value_list = new_value.split(";") if isinstance(new_value, str) and ";" in new_value else new_value

                update_criteria = {field_to_match: value_to_match}
                previous_document = collection.find_one(update_criteria)

                # Check if the document exists
                if previous_document:
                    # Retrieve the current value using dot notation
                    current_value = previous_document
                    for key in update_field.split("."):
                        current_value = current_value.get(key)
                        if current_value is None:
                            break

                    # If the field doesn't exist or is None, create it and set the new value
                    if current_value is None:
                        print(f"Field '{update_field}' doesn't exist or has no value in document with {field_to_match}: {value_to_match}. Creating field and setting new value.")
                        updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, None, new_value_list)
                        result = collection.update_one(update_criteria, {"$set": {update_field: new_value_list, "log": updated_log}})
                        if result.modified_count > 0:
                            updates_made_csv += 1
                    # If the field exists and has a different value, update it
                    elif current_value != new_value_list:
                        print(f"Field '{update_field}' already exists and has a different value in document with {field_to_match}: {value_to_match}. Updating the field.")
                        updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, current_value, new_value_list)
                        result = collection.update_one(update_criteria, {"$set": {update_field: new_value_list, "log": updated_log}})
                        if result.modified_count > 0:
                            updates_made_csv += 1
                    # If the field exists and has the same value, no update is needed
                    else:
                        print(f"Field '{update_field}' already exists and has the same value in document with {field_to_match}: {value_to_match}. No update required.")
                
                # If the document doesn't exist, create a new one with the specified field and value
                else:
                    print(f"The document with {field_to_match} {value_to_match} is not in the collection. Creating new document, new field and setting new value.")
                    temp_document = {field_to_match: value_to_match}
                    
                    # Create the nested structure for the update_field
                    keys = update_field.split(".")
                    nested = temp_document
                    # If the field is nested, create the necessary structure
                    for key in keys[:-1]:
                        nested[key] = {}
                        nested = nested[key]
                    nested[keys[-1]] = new_value_list
                    collection.insert_one(temp_document)
                    updates_made_csv += 1
                    updated_log = log_functions.updateLog(temp_document, process_id, operation, update_field, None, new_value_list)
                    collection.update_one({field_to_match: value_to_match}, {"$set": {"log": updated_log}})

            if updates_made_csv > 0:
                print(f"Total number of updates made with this CSV: {updates_made_csv}.")
            else:
                log_functions.deleteLog(db, str(process_id))
                print(f"No changes were made with CSV files.")

        ### Process JSON files ###
        for json_file in json_files:
            print(f"Processing {json_file}")

            with open(json_file) as f:
                update_documents = json.load(f)

            # Ensure the JSON is a list of documents
            if isinstance(update_documents, dict):
                update_documents = [update_documents]
            # If the JSON is not a list, skip processing
            elif not isinstance(update_documents, list):
                print(f"Invalid JSON format in {json_file}.")
                continue
            
            # Insert metadata about the update process in the log_details collection
            process_id = log_functions.insertLog(db, name, method, operation, collection_name)
            updates_made_json = 0

            # Loop through each document in the JSON
            for update_doc in update_documents:
                # Ensure the document has a stable_id for matching
                if "stable_id" not in update_doc:
                    print("Skipping document, missing 'stable_id'.")
                    continue

                # Build the query to find the document
                query = {"stable_id": update_doc["stable_id"]}
                old_doc = collection.find_one(query)

                # If the document exists, update it
                if old_doc:              
                    updates = {}
                    # Process each field in the update document
                    for field, new_value in update_doc.items():
                        if field in ["stable_id", "log"]:
                            continue
                        if "." in field:
                            # Build nested dict for dotted fields (basic support)
                            keys = field.split(".")
                            nested = updates
                            for k in keys[:-1]:
                                nested = nested.setdefault(k, {})
                            nested[keys[-1]] = new_value
                        else:
                            updates[field] = new_value

                    # If no updates are found, skip this document
                    if not updates:
                        print(f"No update fields found in document with stable_id '{update_doc['stable_id']}'. Skipping.")
                        continue
                
                    # Apply update
                    collection.update_one(query, {"$set": updates})

                    # Compare and generate log entry
                    new_doc = collection.find_one(query)
                    log_entry = log_functions.diffLogEntry(old_doc, new_doc, process_id, operation)

                    # If there are modified fields, log the changes
                    if log_entry["modified_fields"]:
                        field_names = [f["field"] for f in log_entry["modified_fields"]]
                        print(f"Updated document with stable_id: {update_doc['stable_id']}. Modified fields: {', '.join(field_names)}.")
                        collection.update_one(query, {"$push": {"log": {"$each": [log_entry],"$position": 0}}})
                        updates_made_json += len(field_names)
                    else:
                        print(f"No differences found in document with stable_id '{update_doc['stable_id']}'.")
                
                # If the document doesn't exist, create a new one
                else:
                    print(f"The document with stable_id {update_doc['stable_id']} is not in the collection. Creating new document, new field(s) and setting new value(s).")
                    new_doc = {k: v for k, v in update_doc.items() if k != "log"}
                    modified_fields = [{"field": k} for k in new_doc if k not in ["stable_id", "log"]] # Collect modified fields

                    # Create a log entry for the new document
                    log_entry = {
                        "log_id": str(process_id),
                        "operation": operation,
                        "modified_fields": modified_fields
                    }
                    new_doc["log"] = [log_entry]
                    collection.insert_one(new_doc)
                    updates_made_json += len(new_doc) - 3                    

            if updates_made_json > 0:
                print(f"Total number of updates made with this JSON: {updates_made_json}.")
            else:
                log_functions.deleteLog(db, str(process_id))
                print(f"No changes were made with this JSON file.")