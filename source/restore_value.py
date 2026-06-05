#!/usr/bin/env python

"""restore.py  :  Reset to previous version as saved in meta """

__author__ = "Marta Huertas"
__version__ = "0.1.2"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

from . import log_functions

def restoreOne(operation, db, collection_name, restore_criteria, log_id, name, method):
    """
    Restore all fields modified in the target log entry for one document.
    The fields to restore are taken from the modified_fields section of the matching log entry.
    One restore log entry is created for the document.
    """
    # Access the collection
    collection = db[collection_name]

    # Find the document
    doc = collection.find_one(restore_criteria)

    if not doc:
        print("The document you are searching for is not in the collection.")
        return

    # Insert metadata about the restore process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Retrieve logs
    logs = doc.get("log", [])

    # Find the target log entry by log_id
    target_log_index = None
    for i, log in enumerate(logs):
        if log.get("log_id") == log_id:
            target_log_index = i
            break

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

    # Get fields to restore from the target log entry
    target_modified_fields = logs[target_log_index].get("modified_fields", [])

    if not target_modified_fields:
        log_functions.deleteLog(db, str(process_id))
        print("No modified fields found in target log.")
        return

    update_fields = {}
    new_modified_fields = []

    # Restore each field present in the target log entry
    for target_field_entry in target_modified_fields:
        field_name = target_field_entry.get("field")

        if not field_name:
            continue

        # Get current field value
        current_value = doc
        for key in field_name.split("."):
            current_value = current_value.get(key, None) if isinstance(current_value, dict) else None
            if current_value is None:
                break

        # Initialize restored_value as a list
        restored_value = (
            current_value
            if isinstance(current_value, list)
            else [current_value]
            if current_value is not None
            else []
        )

        looped_logs = 0

        # Loop through logs from latest to target log, INCLUDING target log
        for log in logs:
            looped_logs += 1
            print(f"Processing log entry {looped_logs} for field '{field_name}'")

            field_entry = next(
                (mf for mf in log.get("modified_fields", []) if mf.get("field") == field_name),
                None
            )

            if field_entry:
                added = field_entry.get("added", [])
                removed = field_entry.get("removed", [])

                print(f"Current value from this log entry: {restored_value}")
                print(f"Added values: {added}")
                print(f"Removed values: {removed}")

                # Reverse the change: remove added, re-add removed
                restored_value = [x for x in restored_value if x not in added] + removed

                print(f"Restored value after processing this log entry: {restored_value}")

            if log.get("log_id") == log_id:
                print(f"Reached target log entry with log_id: {log_id}. Stopping log processing for field '{field_name}'.")
                break

        # Convert back to scalar only if the original field was not a list
        if not isinstance(current_value, list) and isinstance(restored_value, list) and len(restored_value) <= 1:
            restored_value = restored_value[0] if restored_value else None

        # Store only actual changes
        if restored_value != current_value:
            update_fields[field_name] = restored_value

            current_as_list = (
                current_value
                if isinstance(current_value, list)
                else [current_value]
                if current_value is not None
                else []
            )

            restored_as_list = (
                restored_value
                if isinstance(restored_value, list)
                else [restored_value]
                if restored_value is not None
                else []
            )

            new_modified_fields.append({
                "field": field_name,
                "added": restored_as_list,
                "removed": current_as_list
            })

    # If no fields changed, delete process log and exit
    if not update_fields:
        log_functions.deleteLog(db, str(process_id))
        print("No changes needed. Fields already match target state.")
        return

    # Create one new restore log entry for all restored fields
    new_log_entry = {
        "log_id": str(process_id),
        "operation": operation,
        "modified_fields": new_modified_fields
    }

    # Keep newest-to-oldest order
    updated_log = [new_log_entry] + logs

    # Update the document
    result = collection.update_one(
        restore_criteria,
        {
            "$set": {
                **update_fields,
                "log": updated_log
            }
        }
    )

    if result.modified_count:
        restored_field_names = ", ".join(update_fields.keys())
        print(f"Fields successfully restored to the values at log_id {log_id}: {restored_field_names}.")
    else:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")

def restoreAll(operation, db, collection_name, log_id, name, method):
    """
    Reset all fields modified in the target log entry for all documents in the collection using the log_id.
    """
    # Access the collection
    collection = db[collection_name]

    # Insert metadata about the restore process in the log_details collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    restored_documents = 0

    # Iterate through all documents in the collection
    for doc in collection.find():
        logs = doc.get("log", [])
        target_log_index = None

        # Find the target log entry by log_id
        for i, log in enumerate(logs):
            if log.get("log_id") == log_id:
                target_log_index = i
                break

        # Skip documents where the log_id is not found
        if target_log_index is None:
            print(f"log_id {log_id} not found in document {doc.get('stable_id')}")
            continue

        # Check if the operation is valid for restoration
        log_operation = logs[target_log_index].get("operation", "").lower()
        if not (log_operation.startswith("update") or log_operation.startswith("restore")):
            print(f"Only update or restore operations can be restored. Skipping document {doc.get('stable_id')}")
            continue

        # Get modified fields from the target log
        target_modified_fields = logs[target_log_index].get("modified_fields", [])
        if not target_modified_fields:
            print(f"No modified fields found in target log for document {doc.get('stable_id')}")
            continue

        update_fields = {}
        new_modified_fields = []

        # Process each field mentioned in the target log
        for target_field_entry in target_modified_fields:
            field_name = target_field_entry.get("field")
            if not field_name:
                continue

            # Get current field value
            current_value = doc
            for key in field_name.split("."):
                current_value = current_value.get(key, None) if isinstance(current_value, dict) else None
                if current_value is None:
                    break

            # Initialize restored_value
            restored_value = current_value if isinstance(current_value, list) else [current_value] if current_value is not None else []

            looped_logs = 0

            # Process logs from newest to oldest, INCLUDING the target log
            for log in logs:
                looped_logs += 1
                print(f"Processing log entry {looped_logs} for field '{field_name}' in document {doc.get('stable_id')}")

                field_entry = next(
                    (mf for mf in log.get("modified_fields", []) if mf.get("field") == field_name),
                    None
                )

                if field_entry:
                    added = field_entry.get("added", [])
                    removed = field_entry.get("removed", [])

                    restored_value = [x for x in restored_value if x not in added] + removed
                    #print(f"Restored value after processing this log entry: {restored_value}")

                if log.get("log_id") == log_id:
                    #print(f"Reached target log entry with log_id: {log_id}. Stopping log processing for field '{field_name}'.")
                    break

            # Convert back to scalar only if original field was not a list
            if not isinstance(current_value, list) and isinstance(restored_value, list) and len(restored_value) <= 1:
                restored_value = restored_value[0] if restored_value else None

            # Keep only fields that actually changed
            if restored_value != current_value:
                update_fields[field_name] = restored_value

                # Build one entry for the combined restore log
                current_as_list = current_value if isinstance(current_value, list) else [current_value] if current_value is not None else []
                restored_as_list = restored_value if isinstance(restored_value, list) else [restored_value] if restored_value is not None else []

                new_modified_fields.append({
                    "field": field_name,
                    "added": restored_as_list,
                    "removed": current_as_list
                })

        # If no fields changed, skip this document
        if not update_fields:
            print(f"No changes needed for document {doc.get('stable_id')}.")
            continue

        # Create one new log entry for the whole restore
        current_log = doc.get("log", [])
        new_log_entry = {
            "log_id": str(process_id),
            "operation": operation,
            "modified_fields": new_modified_fields
        }

        # Keep newest-to-oldest ordering
        updated_log = [new_log_entry] + current_log

        # Update document
        result = collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {**update_fields, "log": updated_log}}
        )

        if result.modified_count:
            restored_documents += 1

    # Final result
    if restored_documents == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f"Fields from log {log_id} were successfully restored in {restored_documents} documents.")