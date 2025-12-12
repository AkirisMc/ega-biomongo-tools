#!/usr/bin/env python

"""restore.py  :  Reset to previous version as saved in meta """

__author__ = "Marta Huertas"
__version__ = "0.1.2"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

from . import log_functions

def restoreOne(operation, db, collection_name, reset_criteria, field_name, log_id, name, method):
    """
    Reset the value of a field (embedded or non-embedded) in a document to a previous version using the log_id.
    """
    # Access the collection:
    collection = db[collection_name]

    # Find the document
    doc = collection.find_one(reset_criteria)
    
    # Check if the document exists
    if not doc:
        print(f"The document you are searching for is not in the collection.")
        return

    # Insert metadata about the restore process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Retrieve the logs
    logs = doc.get('log', [])

    # Find the target log entry by log_id
    target_log_index = None # Set to None to indicate not found
    for i, log in enumerate(logs):
        if log.get("log_id") == log_id:
            target_log_index = i
            break

    # If the log_id is not found, delete the log and exit
    if target_log_index is None:
        log_functions.deleteLog(db, str(process_id))
        print(f"log_id {log_id} not found in document.")
        return
    
    # Check if the operation is valid for restoration
    log_operation = logs[target_log_index].get("operation", "").lower()
    if not (log_operation.startswith("update") or log_operation.startswith("restore")):
        log_functions.deleteLog(db, str(process_id))
        print("Only update or restore operations can be restored.")
        return

    # Get current field value
    current_value = doc
    for key in field_name.split("."):
        current_value = current_value.get(key, None) if isinstance(current_value, dict) else None
        if current_value is None:
            break
    
    # Initialize restored_value as a list
    restored_value = current_value if isinstance(current_value, list) else [current_value] if current_value is not None else [] 

    looped_logs = 0 # Counter for logs processed

    # Loop through logs from latest to target log
    for log in logs:
        if log.get("log_id") == log_id:
            break

        looped_logs += 1
        print(f"Processing log entry {looped_logs}")

        # Find the field entry in the modified fields
        field_entry = next((mf for mf in log.get("modified_fields", []) if mf.get("field") == field_name), None) 
        if not field_entry:
            continue

        added = field_entry.get("added", []) # List of values added in this log entry
        removed = field_entry.get("removed", []) # List of values removed in this log entry

        print(f"Current value from this log entry: {restored_value}")
        print(f"Added values: {added}")
        print(f"Removed values: {removed}") 

        # Reverse the change: remove added, re-add removed
        restored_value = [x for x in restored_value if x not in added] + removed
        print(f"Restored value after processing this log entry: {restored_value}")

    # Convert back to original type if it was not a list
    if not isinstance(current_value, list) and len(restored_value) <= 1:
        restored_value = restored_value[0] if restored_value else None

    # Check if the restored value is the same as the current value
    if restored_value == current_value:
        log_functions.deleteLog(db, str(process_id))
        print("No changes needed. Field already matches target state.")
        return

    # Update the document
    updated_log = log_functions.updateLog(doc, process_id, operation, field_name, current_value, restored_value)
    result = collection.update_one(reset_criteria, {"$set": {field_name: restored_value, "log": updated_log}})

    # Check if the update was successful
    if result.modified_count:
        print(f"Field {field_name} successfully restored to the value at log_id: {log_id}.")
    else:
        print("No changes were made.")

def restoreAll(operation, db, collection_name, field_name, log_id, name, method):
    """
    Reset a field (embedded or non-embedded) in all documents in the collection to a previous version using log_id.
    """
    # Access the collection:
    collection = db[collection_name]

    # Insert metadata about the restore process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    restored_documents = 0
    
    # Iterate through all documents in the collection
    for doc in collection.find():
        # Retrieve the logs 
        logs = doc.get("log", [])
        target_log_index = None

        # Find the target log entry by log_id
        for i, log in enumerate(logs):
            if log.get("log_id") == log_id:
                target_log_index = i
                break
        
        # If the log_id is not found, skip this document
        if target_log_index is None:
            print(f"log_id {log_id} not found in document {doc.get('stable_id')}")
            continue

        # Check if the operation is valid for restoration
        log_operation = logs[target_log_index].get("operation", "").lower()
        if not (log_operation.startswith("update") or log_operation.startswith("restore")):
            print(f"Only update or restore operations can be restored. Skipping document {doc.get('stable_id')}")
            continue

        # Get current field value
        current_value = doc
        for key in field_name.split("."):
            current_value = current_value.get(key, None) if isinstance(current_value, dict) else None
            if current_value is None:
                break

        # Initialize restored_value as a list
        restored_value = current_value if isinstance(current_value, list) else [current_value] if current_value is not None else []

        looped_logs = 0  # Counter for logs processed

        # Loop through logs from latest to target log
        for log in logs:
            if log.get("log_id") == log_id:
                break
            
            looped_logs += 1

            # Find the field entry in the modified fields
            field_entry = next((mf for mf in log.get("modified_fields", []) if mf.get("field") == field_name), None)
            if not field_entry:
                continue

            added = field_entry.get("added", []) # List of values added in this log entry
            removed = field_entry.get("removed", []) # List of values removed in this log entry

            # Reverse the change: remove added, re-add removed
            restored_value = [x for x in restored_value if x not in added] + removed

        # Convert back to original type if needed
        if not isinstance(current_value, list) and len(restored_value) <= 1:
            restored_value = restored_value[0] if restored_value else None

        # Check if the restored value is different from the current value
        if restored_value != current_value:
            # Update the log with the restoration details
            updated_log = log_functions.updateLog(doc, process_id, operation, field_name, current_value, restored_value)
            # Update the document with the restored value and updated log 
            result = collection.update_one({"_id": doc["_id"]}, {"$set": {field_name: restored_value, "log": updated_log}}) 
            if result.modified_count:
                restored_documents += 1

    # Check if any documents were restored
    if restored_documents == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f"Field {field_name} successfully restored to the values at log {log_id} in {restored_documents} documents.")