import time
import fdb

def main():
    print("Initializing FDB API version 800...")
    fdb.api_version(800)
    db = fdb.open()

    stream_name = b"e2e_cdc_stream"
    key_prefix = b"cdc_test:"

    # 1. Register CDC stream over key range
    try:
        db.register_cdc_stream(stream_name, key_prefix, key_prefix + b"\xff").wait()
        print("1. Stream registered successfully!")
    except Exception as e:
        print("Stream registration note:", e)

    # 2. Create consumer
    consumer = db.create_cdc_consumer(stream_name).wait()
    print("2. CDC consumer created successfully!")

    # 3. Write a key to trigger a CDC mutation
    test_key = key_prefix + b"hello"
    test_val = b"world"
    db[test_key] = test_val
    print(f"3. Transaction committed: {test_key.decode()} = {test_val.decode()}")

    # 4. Consume the mutation
    print("4. Polling for CDC mutation batch...")
    found = False
    for attempt in range(5):
        batch = consumer.consume().wait()
        if hasattr(batch, "mutations") and len(batch.mutations) > 0:
            print(f"\n🎉 Successfully consumed CDC batch on attempt {attempt}!")
            print(f"   Commit Version: {batch.last_consumed_version}")
            for vm in batch.mutations:
                for m in vm.mutations:
                    print(f"   Captured Mutation -> Key: {m.param1}, Value: {m.param2}")
            found = True
            break
        time.sleep(0.3)

    if not found:
        print("⚠️ No mutations received within timeout.")

if __name__ == "__main__":
    main()
