#!/usr/bin/env python

"""rename.py  :  Change the name of a field in a specific collection. """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

import json
from . import log_functions
from pymongo import UpdateOne

def removeOne(operation, db, collection_name, remove_criteria, remove_field, name, method):
    """
    Remove a normal field or an embedded field from one document present in a specific collection from the database.
    """

    # Access the collection
    collection = db[collection_name]

    # Insert metadata about the update process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Find the document to update
    doc = collection.find_one(remove_criteria)

    # If the document does not exist, exit the function
    if not doc:
        log_functions.deleteLog(db, str(process_id))
        print(f"Document matching criteria {remove_criteria} not found.")
        return

    # Traverse embedded fields
    keys = remove_field.split(".")
    current = doc # Start from the root of the document
    parent = None # To keep track of the parent dictionary
    last_key = None # To keep track of the last key in the path

    # Navigate to the target field
    for key in keys:
        # If at any point the key does not exist, exit the function
        if not isinstance(current, dict) or key not in current:
            print(f"Field {remove_field} does not exist in document with stable id: {doc['stable_id']}.")
            log_functions.deleteLog(db, str(process_id))
            return
        parent = current # Update parent to current before going deeper
        last_key = key # Update last_key to the current key
        current = current[key] # Move deeper into the document

    
    previous_value = parent[last_key] # Store the previous value for logging

    # Remove the field
    del parent[last_key]

    # Build custom log entry
    log_entry = {
        "log_id": str(process_id),
        "operation": operation,
        "modified_fields": [
            {
                "field": remove_field,
                "removed": previous_value if isinstance(previous_value, list) else [previous_value]
            }
        ]
    }

    # Merge with existing log if present
    existing_log = [] if doc is None else doc.get("log", [])
    existing_log.insert(0, log_entry)

    # Update the document with the new log
    result = collection.update_one({"_id": doc["_id"]}, {"$unset": {remove_field: ""}, "$set": {"log": existing_log}})

    # Provide feedback
    if result.modified_count:
        print(f"Field {remove_field} removed from document with stable id: {doc['stable_id']}.")
    else:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")

def removeAll(operation, db, collection_name, remove_field, name, method):
    """
    Remove a normal field or an embedded field from all the documents present in a specific collection from the database.
    """

    # Access the collection
    collection = db[collection_name]

    # Insert metadata about the update process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    bulk_updates = [] # To store bulk update operations

    # Fetch all documents in the collection
    prev_docs = collection.find()

    # Loop through each document
    for doc in prev_docs:

        # Traverse embedded fields
        keys = remove_field.split(".")
        current = doc # Start from the root of the document
        parent = None # To keep track of the parent dictionary
        last_key = None # To keep track of the last key in the path

        # Navigate to the target field
        for key in keys:
            # If at any point the key does not exist, skip to the next document
            if not isinstance(current, dict) or key not in current:
                parent = None
                break
            parent = current # Update parent to current before going deeper
            last_key = key # Update last_key to the current key
            current = current[key] # Move deeper into the document
        
        # If the field does not exist, skip to the next document
        if parent is None:
            continue
        
        previous_value = parent[last_key] # Store the previous value for logging
        
        # Build custom log entry
        log_entry = {
            "log_id": str(process_id),
            "operation": operation,
            "modified_fields": [
                {
                    "field": remove_field,
                    "removed": previous_value if isinstance(previous_value, list) else [previous_value]
                }
            ]
        }
        
        # Merge with existing log if present
        existing_log = [] if doc is None else doc.get("log", [])
        existing_log.insert(0, log_entry)
        
        # Prepare bulk update
        bulk_updates.append(UpdateOne({"_id": doc["_id"]}, {"$unset": {remove_field: ""}, "$set": {"log": existing_log}}))

    # Execute bulk update if there are any updates to be made
    if bulk_updates:
        result = collection.bulk_write(bulk_updates)
        updated_count = result.modified_count or len(bulk_updates)
    else:
        updated_count = 0

    # Provide feedback
    if updated_count == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f"{updated_count} document(s) updated successfully. Removed field: {remove_field}")