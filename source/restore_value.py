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

    # Find the target field entry in the log
    target_field_log = next((mf for mf in logs[target_log_index].get("modified_fields", [])
                             if mf.get("field") == field_name), None)
    if not target_field_log:
        log_functions.deleteLog(db, str(process_id))
        print(f"Field '{field_name}' not found in log {log_id}.")
        return

    # Get current field value
    current_value = doc
    for key in field_name.split("."):
        current_value = current_value.get(key, None) if isinstance(current_value, dict) else None
        if current_value is None:
            break

    # Normalize to list for easier added/removed operations
    if isinstance(current_value, list):
        restored_value = current_value.copy()
    elif current_value is None:
        restored_value = []
    else:
        restored_value = [current_value]

    # Walk backwards through logs from latest down to (and including) target log
    for log in reversed(logs[target_log_index:]):
        field_entry = next((mf for mf in log.get("modified_fields", [])
                            if mf.get("field") == field_name), None)
        if not field_entry:
            continue

        added = field_entry.get("added", [])
        removed = field_entry.get("removed", [])

        # Reverse the change: remove 'added' values, re-add 'removed' values
        restored_value = [x for x in restored_value if x not in added] + removed

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
        print(f"Restored '{field_name}' to the value from log {log_id}.")
    else:
        print("No changes were made.")

def restoreAll(operation, db, collection_name, log_id, name, method):
    """
    Reset a field (embedded or non-embedded) in all documents in the collection to a previous version using log_id.
    """
    # Access the collection:
    collection = db[collection_name]

    # Retrieve all documents in the collection
    documents = collection.find()

    # Insert metadata about the restore process in the meta collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)
    if not process_id:
        print('Failed to create log for the restore process.')
        return

    restored_documents = 0
    
    for document in documents:
        log_entries = document.get('log', [])
        
        # Find the log entry to restore
        log_entry = next((entry for entry in log_entries if entry.get('log_id') == log_id), None)
        
        if not log_entry:
            print(f"The log_id {log_id} does not exist in the document with _id {document['_id']}")
            continue

        if 'update' not in log_entry.get('operation') and 'restore' not in log_entry.get('operation'):
            print(f"Only update|restore operations can be restored in document with _id {document['_id']}")
            continue

        # Get the field and value to restore
        modified_field = log_entry.get('modified_field')

        # Retrieve current value (handle embedded fields)
        current_value = document
        for key in modified_field.split("."):
            current_value = current_value.get(key, None)
            if current_value is None:
                break
        
        current_value_list = current_value if isinstance(current_value, list) else [current_value] if current_value is not None else []
        
        looped_logs = 0
        restored_value_list = current_value_list

        # Loop through log entries to compute the restored value
        for entry in log_entries:
            if entry.get('log_id') == log_id:
                break

            if entry.get('modified_field') == modified_field:
                looped_logs += 1
                added_value_list = entry.get('changed_values', {}).get('added', [])
                removed_value_list = entry.get('changed_values', {}).get('removed', [])

                # Compute the restored value
                restored_value_list = [x for x in restored_value_list if x not in added_value_list] + removed_value_list
        
        # Convert single-element lists to a normal string
        restored_value_list = restored_value_list[0] if len(restored_value_list) == 1 else restored_value_list

        # Check if the restored value is equal to the current value
        if restored_value_list == current_value or restored_value_list == current_value_list:
            print(f"No changes needed for document stable_id {document['stable_id']}, the current value is already equal to the restored value.")
            continue

        # Create the log object for this operation
        updated_log = log_functions.updateLog(document, process_id, operation, modified_field, current_value, restored_value_list)

        # Update the document
        result = collection.update_one(
            {"_id": document["_id"]},
            {"$set": {modified_field: restored_value_list, "log": updated_log}}
        )

        if result.modified_count > 0:
            restored_documents += 1
    
    if restored_documents == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f'Field {modified_field} restored successfully to the values at log {log_id} in {restored_documents} documents.')