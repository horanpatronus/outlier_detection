import pandas as pd
import os
import statistics
from itertools import combinations

# Mendapatkan path absolut dari direktori saat ini
current_directory = os.path.dirname(__file__)

# Mengonstruksi path absolut ke folder data
data_folder_path = os.path.join(current_directory, 'data_chebyshev')

# List semua file dalam folder data
files = os.listdir(data_folder_path)

# Filter hanya file dengan ekstensi .csv
csv_files = [file for file in files if file.endswith('.csv')]

# Inisialisasi variabel
W = 10
distance_chebyshev = 0
distance_list = []
total = 0
iterasi = 1

# Proses setiap file CSV dalam folder data
for csv_file in csv_files:
    data_file_path = os.path.join(data_folder_path, csv_file)
    
    # Read CSV file using pandas
    df_train = pd.read_csv(data_file_path)

    # Check if 'nsb' column exists
    if 'nsb' not in df_train.columns:
        print(f"Kolom 'nsb' tidak ditemukan di file {csv_file}. Kolom yang tersedia: {df_train.columns.tolist()}")
        continue

    class Entitas:
        def __init__(self, object):
            self.object = object
            self.distance = 0

    # Inisialisasi objek entitas dari DataFrame
    entitas_train_list = []
    for _, row in df_train.iterrows():
        entitas_train = Entitas(object=row['nsb'])
        entitas_train_list.append(entitas_train)

    # Inisialisasi window
    window = []

    # Iterasi melalui data stream
    for i, data in enumerate(entitas_train_list):
        # Masukkan data ke dalam window
        window.append(data)

        # Jika window sudah mencapai ukuran W
        if len(window) == W:
            # Hitung jarak antara setiap pasangan data dalam window
            for pair in combinations(window, 2):
                index1, index2 = pair
                distance = abs(index1.object - index2.object)
                distance_list.append(distance)
                total = total + distance
                iterasi = iterasi + 1

                if distance > distance_chebyshev:
                    distance_chebyshev = distance

            # Sliding window
            window = window[1:]

# print(f"Total jarak: {total}")
# print(f"Jarak Chebyshev: {distance_chebyshev}")
# print(f"Jumlah iterasi: {iterasi}")
