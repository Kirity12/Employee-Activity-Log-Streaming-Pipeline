from kafka import KafkaProducer
import json, time
import kagglehub
import pandas as pd


dasgroup_rba_dataset_path = kagglehub.dataset_download("dasgroup/rba-dataset")
chunk_index = 0


def get_data():
    global chunk_index
    chunksize = 1000

    chunk_generator = pd.read_csv(
        dasgroup_rba_dataset_path + "\\rba-dataset.csv", chunksize=chunksize
    )
    for _ in range(chunk_index):
        next(chunk_generator)

    try:
        # Get the next chunk
        chunk = next(chunk_generator)
        chunk_index += 1

        return json.dumps(chunk.to_dict(orient="records"))
    except StopIteration:
        chunk_index = 0
        return None


def stream_data(**context):
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"], max_block_ms=5000, api_version=(7, 4, 0)
    )
    task_id = "Mock"
    curr_time = time.time()
    i = 0

    while True:
        try:
            response = get_data()
            data1 = json.loads(response)
            for data in data1:
                i += 1
                try:
                    producer.send("users_created", json.dumps(data).encode("utf-8"))
                    print(f"Task {task_id}: Message Data {i} sent to Kafka Topic")
                except:
                    print(
                        f"Task {task_id}: Message Data {i} failed to send to Kafka Topic"
                    )
                if i % 10 == 0:
                    time.sleep(3)

            print(f"Task {task_id}: Data Production to Kafka succefully completed")

        except Exception as e:
            print(f"Task {task_id}: An error occurred: {e}")
            break


if __name__=="__main__": 
    stream_data(**{})
