#!/usr/bin/env python

""" meta.py  :  Generate meta information about the process """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"


# Import Packages
from datetime import datetime
from bson.objectid import ObjectId
from deepdiff import DeepDiff


def insertLog(db, name, method, operation, collection_name):
    """
    Generate a log document with information about the process inside the log_details collection.
    """
    # Insert metadata into the 'log_details' collection
    log_collection = db['log_details']
    process_info = {
        # The id is created by MongoDB, no need to generate one.
        "name": name,
        "operation" : operation,
        "collection": collection_name,
        "method": method, 
        "date": datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')
    }
    log_result = log_collection.insert_one(process_info)

    return log_result.inserted_id

def updateLog(previous_document, process_id, operation, update_field, previous_value, new_value):
    """
    Generate/update log information inside of every document.
    """

    # If there's no previous document (meaning the log doesn't exist yet), start with an empty log
    existing_log = [] if previous_document is None else previous_document.get("log", [])

    # Normalize values to lists for proper comparison
    prev_list = previous_value if isinstance(previous_value, list) else [previous_value] if previous_value is not None and previous_value != "Non-existing" else []
    new_list = new_value if isinstance(new_value, list) else [new_value] if new_value is not None else []

    # Compute added and removed values
    added_values = list(set(new_list) - set(prev_list))
    removed_values = list(set(prev_list) - set(new_list))

    # Prepare log entry
    new_log = {
        "log_id": str(process_id),
        "operation": operation,
        "modified_field": update_field,
    }

    # Include changed values only if there's a difference
    if previous_value is not None and previous_value != "Non-existing":
        new_log["changed_values"] = {}

        if added_values:  
            new_log["changed_values"]["added"] = added_values  

        if removed_values:  
            new_log["changed_values"]["removed"] = removed_values  


    # Merge the new metadata with the existing log
    existing_log.insert(0, new_log)

    return existing_log

def deleteLog(db, process_id):
    """
    Delete the log document inside the log_details collection based on process_id.
    """
    log_collection = db["log_details"]  
    result = log_collection.delete_one({"_id": ObjectId(process_id)})

def diffLogEntry(old_doc, new_doc, process_id, operation):
    """
    Compare two documents and return a structured log entry listing all changed or newly added fields.
    Special handling for lists of dictionaries.    
    """
    modified_fields = []

    # Flatten both old and new docs
    flat_old = flatten_dict(old_doc)
    flat_new = flatten_dict(new_doc)
    
    # Use union of keys from both documents
    all_keys = set(flat_old.keys()).union(set(flat_new.keys()))

    # Iterate through all keys to find differences
    # Exclude _id and log fields from the comparison
    for key in all_keys:
        if key.startswith("_id") or key.startswith("log"):
            continue

        # Get values from both documents, defaulting to "Non-existing" if the key is not present
        old_value = flat_old.get(key, "Non-existing")
        new_value = flat_new.get(key, "Non-existing")

        # Case 1: Field was added or removed
        if old_value == "Non-existing" or new_value == "Non-existing":
            modified_fields.append({"field": key})
            continue

        # Case 2: Special handling for list of dictionaries
        if isinstance(old_value, list) and isinstance(new_value, list):
            if all(isinstance(i, dict) for i in old_value + new_value):
                entry = compare_list_of_dicts(old_value, new_value, key)
                if entry:
                    modified_fields.append(entry)
                continue  # Skip default comparison

        # Case 3: Generic diff logic for scalar or list values
        if old_value != new_value:
            field_log = {"field": key}

            try:
                # Attempt to compare lists directly
                old_list = old_value if isinstance(old_value, list) else [old_value]
                new_list = new_value if isinstance(new_value, list) else [new_value]

                added = list(set(new_list) - set(old_list))
                removed = list(set(old_list) - set(new_list))
            except TypeError:
                # Unhashable values fallback
                added = [new_value]
                removed = [old_value]


            if added:
                field_log["added"] = added
            if removed:
                field_log["removed"] = removed

            modified_fields.append(field_log)

    return {
        "log_id": str(process_id),
        "operation": operation,
        "modified_fields": modified_fields
    }

def flatten_dict(d, parent_key='', sep='.'):
    """
    Recursively flattens a nested dictionary using dot notation.
    Example: {'a': {'b': 1}} => {'a.b': 1}
    """
    items = []
    # Iterate through the dictionary
    # If the value is a dictionary, recursively flatten it
    # Otherwise append the key-value pair to the items list
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    # Return a flattened dictionary
    return dict(items)

def compare_list_of_dicts(old_list, new_list, field_name):
    """
    Compares two lists of dictionaries and returns a structured log entry
    showing added and removed dictionaries.
    """
    # Normalize the dictionaries to ensure consistent comparison
    # Sort the items in each dictionary to avoid order issues
    def normalize(d):
        return tuple(sorted(d.items())) # Convert dict to tuple for hashing

    # Convert both lists to sets of normalized dictionaries
    old_set = set(map(normalize, old_list))
    new_set = set(map(normalize, new_list))

    added_dicts = [dict(t) for t in new_set - old_set] # Convert back to dict for output
    removed_dicts = [dict(t) for t in old_set - new_set] # Convert back to dict for output

    # If there are added or removed dictionaries, create a structured entry
    if added_dicts or removed_dicts:
        entry = {"field": field_name}
        if added_dicts:
            entry["added"] = added_dicts
        if removed_dicts:
            entry["removed"] = removed_dicts
        return entry
    return None
