#!/usr/bin/env python

"""main.py  :  Run any operation needed in the mongoDB """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

# Import packages
import sys
from pymongo import MongoClient
import conf
from source import insert, restore, update, new_field, mongoConnection

# Functions
def print_help():
    print('USAGE:')
    print('This script combines different tools to manage the BioMongoDB in EGA')
    print('First thing to do is to write your needs in the conf.py file')
    print('Follow the guidelines in that file.')

def connect_mongo():
    try:
        # Connect to MongoDB
        client = MongoClient(mongoConnection.mongo_host, mongoConnection.mongo_port, username=mongoConnection.username, password=mongoConnection.password, authSource=mongoConnection.auth_source)
        # Acces the database:
        db = client[conf.database_name]
        # If connection successful, print success message
        print("Connection to BioMongoDB established.")

        return db
    except ConnectionError as e:
        # If connection fails, print error message
        print("Failed to connect to MongoDB:", e)

def run_operation():
    """
    Run operations in the BioMongoDB by reading the conf file
    """
    # Get database connection:
    db = connect_mongo()

    if conf.operation == 'insert_one' and conf.json_documents != '':
        insert.insertOne(conf.operation, db, conf.collection_name, conf.json_documents, conf.name, conf.method)

    elif conf.operation == 'insert_many' and conf.json_documents != '':
        insert.insertMany(conf.operation, db, conf.collection_name, conf.json_documents, conf.name, conf.method)
    
    elif conf.operation == 'update_one' and conf.update_field != '' and conf.new_value != '':
        update.updateOne(conf.operation, db, conf.collection_name, conf.update_criteria, conf.update_field, conf.new_value, conf.name, conf.method)
    
    elif conf.operation == 'update_all' and conf.update_field != '' and conf.new_value != '':
        update.updateAll(conf.operation, db, conf.collection_name, conf.update_field, conf.new_value, conf.name, conf.method)

    elif conf.operation == 'update_with_file' and conf.update_file != '':
        update.updateFile(conf.operation, db, conf.collection_name, conf.update_file, conf.name, conf.method)

    elif conf.operation == 'restore_one' and conf.restore_criteria != '' and conf.log_id != '':
        restore.restoreOne(conf.operation, db, conf.collection_name, conf.restore_criteria, conf.log_id, conf.name, conf.method)

    elif conf.operation == 'restore_all' and conf.log_id != '':
        restore.restoreAll(conf.operation, db, conf.collection_name, conf.log_id, conf.name, conf.method)

    elif conf.operation == 'add_empty_field' and conf.new_field != '':
        new_field.addNullField(conf.operation, db, conf.collection_name, conf.new_field, conf.name, conf.method)

    elif conf.operation == 'add_field_with_file' and conf.new_field_file != '':
        new_field.addFieldFile(conf.operation, db, conf.collection_name, conf.new_field_file, conf.name, conf.method)

    else:
        print('Something is missing in the conf.py file')

def main():
    if conf.operation == '' and conf.database_name == '' and conf.collection_name == '' and conf.name == '' and conf.method == '':
        # First print help message just in case.
        print_help()
    elif conf.operation == '' or conf.operation not in ['insert_one', 'insert_many', 'update_one', 'update_all', 'update_with_file', 'restore_one', 'restore_all', 'add_empty_field', 'add_field_with_file']:
        print("Operation is missing or wrong.")
    elif conf.database_name == '':
        print("Database is missing.")
    elif conf.collection_name == '':
        print("Collection name is missing.")
    elif conf.name == '':
        print("Your name is missing.")
    elif conf.method == '':
        print("The method you used to obtain the information is missing.")
    else:
        print(f'Operation: {conf.operation}')
        print(f'Database: {conf.database_name}')

        # Run the operation determined in conf:
        run_operation()

if __name__ == main():
    main()


        