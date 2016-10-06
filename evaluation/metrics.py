#Metrics
import numpy as np
import pandas as pd

def ap_at_k(recommended_items, relevant_items, k):
	is_relevant = np.in1d(recommended_items[:k], relevant_items, assume_unique=True).astype(np.float32)
	score = np.sum(np.cumsum(is_relevant) * is_relevant / np.arange(1, k+1)) / min(k, len(relevant_items))
	assert 0 <= score <= 1
	return score

def precision_at_k(recommended_items, relevant_items, k):
	return float(np.intersect1d(recommended_items[:k], relevant_items).shape[0]) / k

def recall(recommended_items, relevant_items):
	if relevant_items.shape[0] == 0: return 0.0
	return float(np.intersect1d(recommended_items[:30], relevant_items).shape[0]) / relevant_items.shape[0]

def user_success(recommended_items, relevant_items):
	return float(np.intersect1d(recommended_items[:30], relevant_items).shape[0] > 0)

def compute_metrics(S, T, k=5):
    recall_avg, user_success_avg, map_at_k = 0.0, 0.0, 0.0
    for uid in S.index:
        #parse ids
        recommended_items = S.ix[uid].values
        relevant_items = T.ix[uid].values
        
        #compute metrics for a user
        prec = precision_at_k(recommended_items, relevant_items, 5)
        rec = recall(recommended_items, relevant_items)
        
        usucc = user_success(recommended_items, relevant_items)
        
        #update averages
        recall_avg += rec
        user_success_avg += usucc
        map_at_k += ap_at_k(recommended_items, relevant_items, k)
        
        
    recall_avg /= S.shape[0]
    user_success_avg /= S.shape[0]
    map_at_k /= S.shape[0]
    
    return (recall_avg, user_success_avg, map_at_k)

#In caso qualcuno fosse curioso...
def challenge_score(S, T, k=20):
	score = 0.0
	recall_avg, user_success_avg, map_at_20 = 0.0, 0.0, 0.0
	for uid in S.index:
		recommended_items = S.ix[uid].values
		relevant_items = T.ix[uid].values
	
		rec = recall(recommended_items, relevant_items)
		usucc = user_success(recommended_items, relevant_items)
		recall_avg += rec
		user_success_avg += usucc
		map_at_20 += ap_at_k(recommended_items, relevant_items, k)
		score += \
		20 * (
			precision_at_k(recommended_items, relevant_items, 2) +
			precision_at_k(recommended_items, relevant_items, 4) +
			usucc +
			rec
			) + \
		10 * (
			precision_at_k(recommended_items, relevant_items, 6) +
			precision_at_k(recommended_items, relevant_items, 20)
			)
	recall_avg /= S.shape[0]
	user_success_avg /= S.shape[0]
	map_at_20 /= S.shape[0]
	return score