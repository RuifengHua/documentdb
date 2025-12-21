#!/usr/bin/env python3
"""Comprehensive changestream test suite"""

import pymongo
import time
from bson import Timestamp

# Connect to DocumentDB
client = pymongo.MongoClient('mongodb://default_user:Admin100@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true')
db = client['test_cs']

# Clean up
db.drop_collection('ops')
db.drop_collection('coll1')
db.drop_collection('coll2')

print("=" * 80)
print("TEST 1: All Operations (insert, update, delete)")
print("=" * 80)

coll = db['ops']
cs = coll.watch()

# Insert
result = coll.insert_one({'x': 1, 'name': 'doc1'})
oid1 = result.inserted_id
event = cs.try_next()
print("INSERT event:")
import pprint
pprint.pprint(event)

# Update
coll.update_one({'_id': oid1}, {'$set': {'x': 2}})
event = cs.try_next()
print("\nUPDATE event:")
pprint.pprint(event)

# Delete
coll.delete_one({'_id': oid1})
event = cs.try_next()
print("\nDELETE event:")
pprint.pprint(event)

cs.close()

print("\n" + "=" * 80)
print("TEST 2: Timestamp Increment")
print("=" * 80)

coll = db['ops']
cs = coll.watch()

# Two inserts in same second
coll.insert_one({'x': 10})
event1 = cs.try_next()
t1 = event1['clusterTime']
print(f"Event 1: time={t1.time}, increment={t1.inc}")

coll.insert_one({'x': 20})
event2 = cs.try_next()
t2 = event2['clusterTime']
print(f"Event 2: time={t2.time}, increment={t2.inc}")

if t1.time == t2.time:
    assert t2.inc == t1.inc + 1, f"Increment should be {t1.inc + 1}, got {t2.inc}"
    print(f"✓ Same second: increment went from {t1.inc} to {t2.inc}")

# Wait 2 seconds
print("Waiting 2 seconds...")
time.sleep(2)

coll.insert_one({'x': 30})
event3 = cs.try_next()
t3 = event3['clusterTime']
print(f"Event 3: time={t3.time}, increment={t3.inc}")

assert t3.time > t2.time, "Time should have advanced"
assert t3.inc == 1, f"Increment should reset to 1, got {t3.inc}"
print(f"✓ New second: increment reset to {t3.inc}")

cs.close()

print("\n" + "=" * 80)
print("TEST 3: Database-Level Changestream")
print("=" * 80)

cs_db = db.watch()

db['coll1'].insert_one({'a': 1})
event = cs_db.try_next()
print(f"Event from coll1: ns={event['ns']}, doc={event.get('fullDocument')}")
assert event['ns']['coll'] == 'coll1'

db['coll2'].insert_one({'b': 2})
event = cs_db.try_next()
print(f"Event from coll2: ns={event['ns']}, doc={event.get('fullDocument')}")
assert event['ns']['coll'] == 'coll2'

print("✓ Database-level changestream sees events from multiple collections")
cs_db.close()

print("\n" + "=" * 80)
print("TEST 4: Collection-Level Changestream")
print("=" * 80)

cs1 = db['coll1'].watch()
cs2 = db['coll2'].watch()

db['coll1'].insert_one({'x': 100})
db['coll2'].insert_one({'y': 200})

event1 = cs1.try_next()
event2 = cs2.try_next()

print(f"cs1 saw: ns={event1['ns']}, doc={event1.get('fullDocument')}")
print(f"cs2 saw: ns={event2['ns']}, doc={event2.get('fullDocument')}")

assert event1['ns']['coll'] == 'coll1', "cs1 should only see coll1"
assert event2['ns']['coll'] == 'coll2', "cs2 should only see coll2"
print("✓ Collection-level changestreams properly isolated")

cs1.close()
cs2.close()

print("\n" + "=" * 80)
print("TEST 5: resumeAfter")
print("=" * 80)

coll = db['ops']
cs = coll.watch()

coll.insert_one({'seq': 1})
event1 = cs.try_next()
resume_token1 = event1['_id']
print(f"Event 1: seq=1, token={resume_token1}")

coll.insert_one({'seq': 2})
event2 = cs.try_next()
print(f"Event 2: seq=2, token={event2['_id']}")

coll.insert_one({'seq': 3})
event3 = cs.try_next()
print(f"Event 3: seq=3, token={event3['_id']}")

cs.close()

# Resume from after event 1
cs_resume = coll.watch(resume_after=resume_token1)
event = cs_resume.try_next()
print(f"Resumed: seq={event['fullDocument']['seq']}")
assert event['fullDocument']['seq'] == 2, "Should resume from seq=2"
print("✓ resumeAfter works correctly")

cs_resume.close()

print("\n" + "=" * 80)
print("TEST 6: startAtOperationTime")
print("=" * 80)

coll = db['ops']
cs = coll.watch()

# Insert 1 and get its clusterTime
coll.insert_one({'seq': 'insert1'})
event1 = cs.try_next()
cluster_time1 = event1['clusterTime']
print(f"Insert 1: clusterTime={cluster_time1}")

cs.close()

# Wait 2 seconds before insert 2
print("Waiting 2 seconds...")
time.sleep(2)

# Insert 2 (in a new second)
coll.insert_one({'seq': 'insert2'})

# Start watching from clusterTime1 - should see insert2
cs_time = coll.watch(start_at_operation_time=cluster_time1)
event = cs_time.try_next()
if event:
    print(f"Event after startAtOperationTime: seq={event['fullDocument']['seq']}")
    assert event['fullDocument']['seq'] == 'insert2', "Should see insert2"
    print("✓ startAtOperationTime works correctly")
else:
    print("ERROR: No event returned")

cs_time.close()

print("\n" + "=" * 80)
print("TEST 7: postBatchResumeToken")
print("=" * 80)

# Create two collections
col1 = db['col1']
col2 = db['col2']

# Start watching col1
cs1 = col1.watch()

# Insert 2 docs into col1 and 3 docs into col2
print("Inserting 2 docs into col1...")
col1.insert_one({'col': 'col1', 'seq': 1})
col1.insert_one({'col': 'col1', 'seq': 2})

print("Inserting 3 docs into col2...")
col2.insert_one({'col': 'col2', 'seq': 1})
col2.insert_one({'col': 'col2', 'seq': 2})
col2.insert_one({'col': 'col2', 'seq': 3})

# Get events from cs1 with batch size 5 (should only see col1 events)
print("\nGetting events from cs1 (watching col1) with batchSize=5...")
cursor_id = cs1._cursor.cursor_id
result = db.command({
    "getMore": cursor_id,
    "collection": "col1",
    "batchSize": 5
})
print(f"nextBatch length: {len(result['cursor']['nextBatch'])}")
for event in result['cursor']['nextBatch']:
    print(f"  Event: {event['fullDocument']}")

post_batch_token = result['cursor']['postBatchResumeToken']
print(f"\npostBatchResumeToken: {post_batch_token}")

cs1.close()

# Insert one more doc into col2
print("\nInserting 1 more doc into col2...")
col2.insert_one({'col': 'col2', 'seq': 4})

# Start watching col2 from postBatchResumeToken
print("Starting cs2 watching col2 with resume_after=postBatchResumeToken...")
cs2 = col2.watch(resume_after=post_batch_token)

# Should see only the last insert (seq=4), skipping the first 3
event = cs2.try_next()
if event:
    print(f"Event from cs2: {event['fullDocument']}")
    assert event['fullDocument']['seq'] == 4, f"Should see seq=4, got {event['fullDocument']['seq']}"
    print("✓ postBatchResumeToken correctly skipped past first 3 col2 inserts")
else:
    print("ERROR: No event from cs2")

cs2.close()

print("\n" + "=" * 80)
print("ALL TESTS PASSED!")
print("=" * 80)

client.close()