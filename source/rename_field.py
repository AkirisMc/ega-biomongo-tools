#!/usr/bin/env python

"""rename.py  :  Change the name of a field in a specific collection. """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

from . import log_functions
from pymongo import UpdateOne

def renameOne(operation, db, collection_name, rename_criteria, rename_field, new_field_name, name, method):
    """
    Rename a normal or embedded field from one document present in a specific collection from the database.
    """

    collection = db[collection_name]

    # Insert metadata about the update process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Find the document
    doc = collection.find_one(rename_criteria)
    if not doc:
        log_functions.deleteLog(db, str(process_id))
        print(f"Document matching criteria {rename_criteria} not found.")
        return

    # Navigate to the old field (supports embedded fields)
    keys = rename_field.split(".")
    current = doc
    parent = None
    last_key = None

    for key in keys:
        if not isinstance(current, dict) or key not in current:
            log_functions.deleteLog(db, str(process_id))
            print(f"Field {rename_field} does not exist in document with stable id: {doc.get('stable_id')}.")
            return
        parent = current
        last_key = key
        current = current[key]

    value = parent[last_key]  # Value of the old field
    del parent[last_key]      # Remove old key locally

    # Assign new field path (support embedded rename)
    new_keys = new_field_name.split(".")
    new_parent = doc
    for key in new_keys[:-1]:
        if key not in new_parent or not isinstance(new_parent[key], dict):
            new_parent[key] = {}
        new_parent = new_parent[key]
    new_parent[new_keys[-1]] = value

    # Build log entry
    log_entry = {
        "log_id": str(process_id),
        "operation": operation,
        "modified_fields": [
            {
                "field": rename_field,
                "renamed_to": new_field_name
            }
        ]
    }

    # Update log in document
    existing_log = [] if doc is None else doc.get("log", [])
    existing_log.insert(0, log_entry)

    # Update the database
    #result = collection.update_one({"_id": doc["_id"]}, {"$set": doc, "$set": {"log": existing_log}})
    result = collection.update_one({"_id": doc["_id"]}, {"$set": {new_field_name: value, "log": existing_log}, "$unset": {rename_field: ""}})

    if result.modified_count:
        print(f"Field {rename_field} renamed to {new_field_name} in document with stable id: {doc.get('stable_id')}.")
    else:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")

def renameAll(operation, db, collection_name, rename_field, new_field_name, name, method):
    """
    Rename a normal or embedded field from all documents in a specific collection in the database.
    """

    collection = db[collection_name]

    # Insert metadata about the update process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    bulk_updates = []
    updated_count = 0

    prev_docs = collection.find()

    # Iterate through all documents
    for doc in prev_docs:

        keys = rename_field.split(".")
        current = doc
        parent = None
        last_key = None

        # Navigate to the old field
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                parent = None
                break
            parent = current
            last_key = key
            current = current[key]

        if parent is None:
            continue

        value = parent[last_key]
        del parent[last_key]

        # Assign new field path
        new_keys = new_field_name.split(".")
        new_parent = doc
        # Create nested structure if needed
        for key in new_keys[:-1]:
            if key not in new_parent or not isinstance(new_parent[key], dict):
                new_parent[key] = {}
            new_parent = new_parent[key]

        new_parent[new_keys[-1]] = value # Set new value

        # Add log entry
        log_entry = {
            "log_id": str(process_id),
            "operation": operation,
            "modified_fields": [
                {
                    "field": rename_field,
                    "renamed_to": new_field_name
                }
            ]
        }

        existing_log = doc.get("log", [])
        existing_log.insert(0, log_entry)
        doc["log"] = existing_log

        bulk_updates.append(UpdateOne({"_id": doc["_id"]}, {"$set": {new_field_name: value, "log": existing_log}, "$unset": {rename_field: ""}}))

    # Execute updates
    if bulk_updates:
        result = collection.bulk_write(bulk_updates)
        updated_count = result.modified_count or len(bulk_updates)
    else:
        updated_count = 0

    # Feedback
    if updated_count == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f"{updated_count} document(s) updated successfully. Renamed field {rename_field} to {new_field_name}.")