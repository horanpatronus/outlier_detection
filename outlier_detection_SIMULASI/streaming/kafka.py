import json
import logging
import pandas as pd
from multiprocessing import Process
from utils import create_producer, create_consumer
from settings import TRANSACTIONS_TOPIC, TRANSACTIONS_CONSUMER_GROUP, ANOMALIES_TOPIC, RESULTS_TOPIC, NUM_PARTITIONS
from chebyshev import distance_chebyshev
# from exact_alg import calculate_distances, outlier_detection
from itertools import combinations
from datetime import datetime, timezone

def detect():
    consumer = create_consumer(topic=TRANSACTIONS_TOPIC, group_id=TRANSACTIONS_CONSUMER_GROUP)
    producer = create_producer()
    window_size = 10
    window = []
    ISB = []
    outlier_list = []
    R = distance_chebyshev / 4
    k = 3
    i = 0

    while True:
        message = consumer.poll(timeout=50)
        if message is None:
            continue
        if message.error():
            logging.error("Consumer error: {}".format(message.error()))
            continue

        record = json.loads(message.value().decode('utf-8')) 
        
        record['count_after'] = []
        record['nn_before'] = []
        record['neighbors'] = 0
        record['category'] = None

        window.append(record)

        if record['id'] == 0 :
            i = 0
            ISB = []
            outlier_list = []
        else :
            i = i + 1

        # inisialisasi t
        t1 = window[0]['id']
        t2 = t1 + window_size - 1

        # Jika window sudah mencapai ukuran W
        if len(window) == window_size:

            for pair in combinations(window, 2):
                index1, index2 = pair
                distance = abs(index1['data'] - index2['data'])

                # cek neighborhood
                if distance <= R:
                    if index2['id'] not in index1['count_after']:
                        index1['count_after'].append(index2['id'])
                    if index1['id'] not in index2['nn_before']:
                        index2['nn_before'].append(index1['id'])                

            for data in window:
                # Nilai dalam nn_before hanya yang setelah t1
                # Nilai dalam count_after hanya yang sebelum t2
                data['nn_before'] = [value for value in data['nn_before'] if value >= t1]
                data['count_after'] = [value for value in data['count_after'] if value <= t2 and value > data['id']]
                data['neighbors'] = len(data['nn_before']) + len(data['count_after'])

                if data['neighbors'] < k :
                    data['category'] = "OUTLIER"
                else :
                    data['category'] = "Inlier"

            last_data = window[-1]
                    
            ISB.append({
                    'id': last_data['id'], 
                    'datetime': last_data['datetime'], 
                    'data': last_data['data'], 
                    'temperature': last_data['temperature'],
                    'neighbors': last_data['neighbors'], 
                    'category': last_data['category'],
                    "current_time": last_data['current_time']
                    })
            
            upload = {
                'id': last_data['id'], 
                'datetime': last_data['datetime'], 
                'data': last_data['data'],
                'temperature': last_data['temperature'],
                'neighbors': last_data['neighbors'], 
                'nn_before': last_data['nn_before'],
                'category': last_data['category'],
                'current_time': last_data['current_time']
                }

            upload = json.dumps(upload).encode("utf-8")

            producer.produce(topic=RESULTS_TOPIC, value=upload)
            producer.flush()

            if last_data['category'] == "OUTLIER":
                
                upload = {
                    'id': last_data['id'],
                    'datetime': last_data['datetime'], 
                    'data': last_data['data'],
                    'temperature': last_data['temperature'],
                    'neighbors': last_data['neighbors'], 
                    'nn_before': last_data['nn_before'],
                    'category': last_data['category'],
                    "current_time": last_data['current_time']
                    }
                
                outlier_list.append(upload)
                upload = json.dumps(upload).encode("utf-8")

                producer.produce(topic=ANOMALIES_TOPIC, value=upload)
                producer.flush()
            
            # Sliding window (hapus satu data terlama)
            window = window[1:]      

            if last_data['data_length'] == i + 1:
                # Mendapatkan tanggal dan waktu saat ini
                timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                # Formatkan nama file CSV dengan menggunakan timestamp
                csv_filename = f"processed_data_results_{last_data['year']}_{last_data['month']}_{timestamp_str}.csv"
                # Simpan DataFrame ke dalam file CSV dengan nama yang sesuai
                df = pd.DataFrame(ISB)
                df.to_csv(csv_filename, index=False)  
                    
                # Formatkan nama file CSV dengan menggunakan timestamp
                csv_filename_outlier = f"processed_data_outlier_{last_data['year']}_{last_data['month']}_{timestamp_str}.csv"
                # Simpan DataFrame ke dalam file CSV dengan nama yang sesuai
                df_outlier = pd.DataFrame(outlier_list)
                df_outlier.to_csv(csv_filename_outlier, index=False)  

    consumer.close()


# One consumer per partition
def main():
    for _ in range(NUM_PARTITIONS):
        p = Process(target=detect)
        p.start()


if __name__ == '__main__':
    main()