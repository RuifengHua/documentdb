#!/usr/bin/env python3
"""
Test script for DocumentDB changestream Phase 1 implementation

# Step 1: Build PostgreSQL Extensions
sudo make install

# Step 2: Build the Gateway
scripts/build_and_install_with_pgrx.sh -i -d pg_documentdb_gw_host/

# Step 3: Start PostgreSQL and the Gateway
scripts/start_oss_server.sh -c -g

# somehow need to manually create the user
psql -h localhost -p 9712 -U documentdb -d postgres -c "SELECT documentdb_api.create_user('{\"createUser\":\"default_user\", \"pwd\":\"Admin100\", \"roles\":[{\"role\":\"readWriteAnyDatabase\",\"db\":\"admin\"}, {\"role\":\"clusterAdmin\",\"db\":\"admin\"}]}');"

# Step 4: Connect and Test

# Using a MongoDB Client
mongosh --host localhost --port 10260 --tls --tlsAllowInvalidCertificates -u default_user -p Admin100

# Using PostgreSQL shell
psql -p 9712 -d postgres

# Terminal 1: Watch logs
tail -f /home/documentdb/.documentdb/data/pglog.log

# Terminal 2: Run test
python3 test_changestream.py


"""

import pymongo
import time
import sys
from pprint import pprint

def test_changestream():
    """Test basic changestream functionality"""
    
    print("Connecting to DocumentDB...")

    # Connect with default_user
    try:
        client = pymongo.MongoClient(
            'mongodb://default_user:Admin100@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true',
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        print("Connected successfully")
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
    
    db = client['test_changestream_db']
    collection = db['test_collection']
    
    # Clean up (skip if auth fails)
    try:
        collection.drop()
        print("Collection cleaned")
    except:
        print("Collection drop skipped")
    
    # Create changestream
    print("\nCreating changestream...")
    try:
        stream = collection.watch()
        print("Changestream created!")

    except Exception as e:
        error_msg = str(e)
        print(f"Failed to create changestream: {e}")
        return False
    
    # Insert two documents
    print("\nInserting two documents...")
    try:
        doc1 = {'name': 'Alice', 'age': 30}
        doc2 = {'name': 'Bob', 'age': 25}
        
        print(f"Command: collection.insert_one({doc1})")
        result1 = collection.insert_one(doc1)
        print(f"✓ Document 1 inserted")
        
        print(f"Command: collection.insert_one({doc2})")
        result2 = collection.insert_one(doc2)
        print(f"✓ Document 2 inserted")
    except Exception as e:
        print(f"Insert failed: {e}")
        return False
    
    # Try to get first change event
    print("\n" + "="*60)
    print("Getting first change event...")
    print("="*60)
    try:
        change1 = stream.try_next()
        if change1:
            print(f"\n✓ Change event 1 received:")
            pprint(dict(change1), indent=2)
        else:
            print("No change event received")
    except Exception as e:
        print(f"Error getting change event 1: {e}")
    
    # Try to get second change event
    print("\n" + "="*60)
    print("Getting second change event...")
    print("="*60)
    try:
        change2 = stream.try_next()
        if change2:
            print(f"\n✓ Change event 2 received:")
            pprint(dict(change2), indent=2)
        else:
            print("No change event received")
    except Exception as e:
        print(f"Error getting change event 2: {e}")
    
    # Wait 2 seconds
    print("\n" + "="*60)
    print("Waiting 2 seconds...")
    print("="*60)
    time.sleep(2)
    
    # Insert third document
    print("\nInserting third document...")
    try:
        doc3 = {'name': 'Charlie', 'age': 35}
        print(f"Command: collection.insert_one({doc3})")
        result3 = collection.insert_one(doc3)
        print(f"✓ Document 3 inserted")
    except Exception as e:
        print(f"Insert failed: {e}")
        return False
    
    # Try to get third change event
    print("\n" + "="*60)
    print("Getting third change event...")
    print("="*60)
    try:
        change3 = stream.try_next()
        if change3:
            print(f"\n✓ Change event 3 received:")
            pprint(dict(change3), indent=2)
        else:
            print("No change event received")
    except Exception as e:
        print(f"Error getting change event 3: {e}")
    
    # Update a document (updatedFields)
    print("\n" + "="*60)
    print("Updating Alice's age...")
    print("="*60)
    try:
        print("Command: collection.update_one({'name': 'Alice'}, {'$set': {'age': 31}})")
        collection.update_one({'name': 'Alice'}, {'$set': {'age': 31}})
        print("✓ Document updated: Alice age -> 31")
    except Exception as e:
        print(f"Update failed: {e}")
        return False
    
    # Try to get update change event
    print("\n" + "="*60)
    print("Getting update change event (updatedFields)...")
    print("="*60)
    try:
        change4 = stream.try_next()
        if change4:
            print(f"\n✓ Update change event received:")
            pprint(dict(change4), indent=2)
        else:
            print("No change event received")
    except Exception as e:
        print(f"Error getting update change event: {e}")
    
    # Update with removedFields
    print("\n" + "="*60)
    print("Removing age field from Alice...")
    print("="*60)
    try:
        print("Command: collection.update_one({'name': 'Alice'}, {'$unset': {'age': ''}})")
        collection.update_one({'name': 'Alice'}, {'$unset': {'age': ''}})
        print("✓ Document updated: Alice age removed")
    except Exception as e:
        print(f"Update failed: {e}")
        return False
    
    # Try to get removedFields change event
    print("\n" + "="*60)
    print("Getting update change event (removedFields)...")
    print("="*60)
    try:
        change_removed = stream.try_next()
        if change_removed:
            print(f"\n✓ Update change event received:")
            pprint(dict(change_removed), indent=2)
        else:
            print("No change event received")
    except Exception as e:
        print(f"Error getting removedFields change event: {e}")
    
    # Update with truncatedArrays
    print("\n" + "="*60)
    print("Adding and truncating array for Charlie...")
    print("="*60)
    try:
        print("Command: collection.update_one({'name': 'Charlie'}, {'$set': {'hobbies': ['reading', 'coding', 'gaming']}})")
        collection.update_one({'name': 'Charlie'}, {'$set': {'hobbies': ['reading', 'coding', 'gaming']}})
        print("✓ Document updated: Charlie hobbies added")
        change_add = stream.try_next()  # consume the add event
        
        print("Command: collection.update_one({'name': 'Charlie'}, {'$pop': {'hobbies': 1}})")
        collection.update_one({'name': 'Charlie'}, {'$pop': {'hobbies': 1}})
        print("✓ Document updated: Charlie hobbies truncated")
    except Exception as e:
        print(f"Update failed: {e}")
        return False
    
    # Try to get truncatedArrays change event
    print("\n" + "="*60)
    print("Getting update change event (truncatedArrays)...")
    print("="*60)
    try:
        change_truncated = stream.try_next()
        if change_truncated:
            print(f"\n✓ Update change event received:")
            pprint(dict(change_truncated), indent=2)
        else:
            print("No change event received")
    except Exception as e:
        print(f"Error getting truncatedArrays change event: {e}")
    
    # Delete a document
    print("\n" + "="*60)
    print("Deleting Bob...")
    print("="*60)
    try:
        print("Command: collection.delete_one({'name': 'Bob'})")
        collection.delete_one({'name': 'Bob'})
        print("✓ Document deleted: Bob")
    except Exception as e:
        print(f"Delete failed: {e}")
        return False
    
    # Try to get delete change event
    print("\n" + "="*60)
    print("Getting delete change event...")
    print("="*60)
    try:
        change5 = stream.try_next()
        if change5:
            print(f"\n✓ Delete change event received:")
            pprint(dict(change5), indent=2)
        else:
            print("No change event received")
    except Exception as e:
        print(f"Error getting delete change event: {e}")
    
    # Close stream
    try:
        stream.close()
        print("\nChangestream closed")
    except Exception as e:
        print(f"⚠ Error closing stream: {e}")
    

    print("="*60)
    print("CHANGESTREAM BASIC TEST COMPLETE!")
    print("="*60)
    
    return True

if __name__ == "__main__":
    success = test_changestream()
    sys.exit(0 if success else 1)
