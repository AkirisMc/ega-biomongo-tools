#!/usr/bin/env python

"""insert.py  :  Insert document in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"


# Import Packages
import json
import re
from pymongo import UpdateOne
from . import log_functions
import os

def insertDocuments(operation, db, collection_name, json_documents, name, method):
    """
    Insert one or multiple documents into a specific collection from a database.
    If the collection doesn't exist, it is created.
    If a document with the same stable_id exists:
        - If the document is identical, it is skipped.
        - If it differs, new fields are merged into the existing document or the existing document is updated (overwritten) with the new fields.
    Otherwise, the document is inserted as new.
    """

    total_inserted_documents = 0  # Counter for tracking total inserted documents
    total_updated_documents = 0  # Counter for tracking total updated documents
    total_skipped_documents = 0  # Counter for tracking total skipped documents
    chunk_size = 10000  # Maximum number of documents to insert in a batch

    if os.path.exists(json_documents):
        # Determine if input is a single file or directory
        if os.path.isfile(json_documents):
            json_files = [json_documents]
            print("There is 1 file to process.")
        elif os.path.isdir(json_documents):
            json_files = [os.path.join(json_documents, f) for f in os.listdir(json_documents) if os.path.isfile(os.path.join(json_documents, f)) and f.endswith('.json')]
            json_files = sorted(json_files, key=lambda s: [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)])
            print(f"There is/are {len(json_files)} file(s) to process.")

        # Access collection
        collection = db[collection_name]
        print(f"Inserting file(s) into {collection_name} collection")

        # Insert metadata about the update process in the log_details collection
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)

        # Loop through each file
        for json_file in json_files:
            print(f"Processing {json_file}")

            with open(json_file) as f:
                data = json.load(f)

            # Determine if data is a single document or a list of documents
            documents = data if isinstance(data, list) else [data]


            # Ensure each document has a stable_id            
            unique_ids = [doc['stable_id'] for doc in documents]
            existing_docs_cursor = collection.find({'stable_id': {'$in': unique_ids}})
            existing_docs = {doc['stable_id']: doc for doc in existing_docs_cursor}

            new_documents = []

            # Process each document            
            for doc in documents:
                stable_id = doc['stable_id']
                # Check if the document already exists in the collection
                if stable_id in existing_docs:
                    existing = existing_docs[stable_id]
                    # Compare full document contents excluding _id and log fields, the copy is just for comparison
                    existing_copy = {k: v for k, v in existing.items() if k != '_id' and k != 'log'}
                    if doc == existing_copy:
                        print(f"Document with stable_id {stable_id} already exists and is identical. Skipping.")
                        total_skipped_documents += 1
                        continue

                    # If the document exists but differs, merge the new document into the existing one
                    print(f"Document with stable_id {stable_id} exists but differs. Overwriting.")
                    merged_doc = existing.copy() # This is an exact copy of the existing document
                    merged_doc.update(doc)  # Simple merge: new values overwrite old ones
                    total_updated_documents += 1

                    # Update log information in the existing document
                    previous_document = existing
                    existing_log = [] if previous_document is None else previous_document.get("log", [])

                    # Prepare new log entry for overwritten document
                    overwritten_log_entry = {
                        "log_id": str(process_id),
                        "operation": operation + "-overwrite"
                    }

                    # Add the new log entry to the existing log
                    existing_log.insert(0, overwritten_log_entry)
                    merged_doc['log'] = existing_log
                    collection.update_one({'_id': existing['_id']}, {'$set': merged_doc})


                # If the document does not exist, prepare it for insertion
                else:
                    new_documents.append(doc)

            # Insert new documents in chunks
            if new_documents:
                for i in range(0, len(new_documents), chunk_size):
                    chunk = new_documents[i:i + chunk_size]
                    result = collection.insert_many(chunk)
                    inserted_ids = result.inserted_ids
                    total_inserted_documents += len(inserted_ids)

                    # If there are inserted documents, prepare log entries
                    if inserted_ids:
                        # Prepare log entry for the inserted documents
                        new_log_entry = {
                            "log_id": str(process_id),
                            "operation": operation
                        }

                        # Add the log entry to each inserted document
                        bulk_updates = [
                            UpdateOne({'_id': doc_id}, {'$set': {'log': [new_log_entry]}}) 
                            for doc_id in inserted_ids
                        ]

                        # Perform bulk update to add log information
                        collection.bulk_write(bulk_updates)

                        print(f"Inserted {len(inserted_ids)} document(s) from chunk {i // chunk_size + 1} of {json_file}.")
                    else:
                        print(f"No new documents to insert from chunk {i // chunk_size + 1} of {json_file}.")

        print(f"Total number of documents inserted: {total_inserted_documents}")
        print(f"Total number of documents updated: {total_updated_documents}")
        print(f"Total number of documents skipped: {total_skipped_documents}")

    else:
        print(f'{json_documents} file or directory does not exist.')