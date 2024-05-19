import os
import json
import time
import pandas as pd
from datetime import datetime, timezone
from settings import TRANSACTIONS_TOPIC, DELAY
from utils import create_producer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class MyHandler(FileSystemEventHandler):
    def __init__(self, producer):
        self.producer = producer

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            try:
                process_file(event.src_path, self.producer)
            except PermissionError as e:
                print(f"Permission error: {e}")
            except Exception as e:
                print(f"Failed to process file: {e}")

def process_file(csv_file_path, producer):
    _id = 0
    try:
        df = pd.read_csv(csv_file_path)
    except PermissionError as e:
        print(f"Permission denied: {e}")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    data_length = len(df)
    df['datetime_column'] = pd.to_datetime(df['received_adjusted'])
    year = df['datetime_column'].dt.year
    month = df['datetime_column'].dt.month
    year_value = str(year.iloc[0])
    month_value = datetime.strptime(str(month.iloc[0]), "%m").strftime("%B")

    record_list = []

    if producer is not None:
        for index, row in df.iterrows():
            current_time = datetime.now(timezone.utc).isoformat()

            record = {
                "id": _id,
                "data": row['nsb'],
                "temperature": row['temperature'],
                "datetime": row['received_adjusted'],
                "current_time": current_time,
                "data_length": data_length,
                "year": year_value,
                "month": month_value
            }
            record_list.append(record)
            record = json.dumps(record).encode("utf-8")

            producer.produce(topic=TRANSACTIONS_TOPIC, value=record)
            producer.flush()
            _id += 1

            time.sleep(DELAY)

    # timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # new_csv_filename = os.path.join("processed_data", f"processed_data_{timestamp_str}.csv")

    # df_ISB = pd.DataFrame(record_list)
    # df_ISB.to_csv(new_csv_filename, index=False)
    # print(f"Processed data saved to {new_csv_filename}")

def process_existing_files(directory, producer):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                process_file(file_path, producer)
            except PermissionError as e:
                print(f"Permission error: {e}")
            except Exception as e:
                print(f"Failed to process file: {e}")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    directory_to_watch = os.path.join(script_dir, 'data')

    if not os.path.exists(directory_to_watch):
        print(f"Directory {directory_to_watch} does not exist.")
        return

    producer = create_producer()
    if producer is None:
        print("Producer creation failed")
        return

    # Process all existing files in the directory
    process_existing_files(directory_to_watch, producer)

    # Set up watchdog to monitor the directory for new files
    event_handler = MyHandler(producer)
    observer = Observer()
    observer.schedule(event_handler, directory_to_watch, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
