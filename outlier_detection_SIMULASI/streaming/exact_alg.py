from itertools import combinations

def calculate_distances(window, R):
    for pair in combinations(window, 2):
        index1, index2 = pair
        distance = abs(index1['data'] - index2['data'])

        # cek neighborhood
        if distance <= R:
            if index2['id'] not in index1['count_after']:
                index1['count_after'].append(index2['id'])
            if index1['id'] not in index2['nn_before']:
                index2['nn_before'].append(index1['id'])

def outlier_detection(window, t1, t2, k):
    for data in window:
        # Nilai dalam nn_before hanya yang setelah t1
        # Nilai dalam count_after hanya yang sebelum t2
        data['nn_before'] = [value for value in data['nn_before'] if value >= t1]
        data['count_after'] = [value for value in data['count_after'] if value <= t2 and value > data['id']]
        data['neighbors'] = len(data['nn_before']) + len(data['count_after'])

        if data['neighbors'] >= k :
            data['category'] = "Inlier"
        else :
            data['category'] = "OUTLIER"