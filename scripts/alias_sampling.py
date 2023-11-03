import numpy as np
import random as rd

def dist_alias_table(P, ids, weights):

    n = len(ids)
    l_weights = []
    l_ids = []
    h_weights = []
    h_ids = []
    W = sum(weights)

    for id, w in zip(ids, weights):
        w_norm = (w / W) * n  # normalize weights so average weight is 1
        if w_norm <= 1:
            l_weights.append(w_norm)
            l_ids.append(id)
        else: 
            h_weights.append(w_norm)
            h_ids.append(id)

    alias_table_paritions = []
    buckets_created = 0 
    h_idx = 0
    l_idx = 0
    spill = 0  # Weight that has spilled over from previous partition(s)
    for p in range(P-1): # Need to handle last case

        partition_size = n // P + (p < (n % P))
        # Need to create partition_size many buckets, where each bucket 
        # has a cumulative weight of 1 = (1 - pr) + pr.
        prtn_w = partition_size
        j = 0  # Number of heavy items to be used by this partitions buckets
        i = partition_size - j # Number of light items to be used by this partitions buckets
        found_ij = False
        while not found_ij:
        
            curr_prtn_w = sum(h_weights[h_idx:h_idx+j]) + sum(l_weights[l_idx:l_idx+i]) + spill
            print(curr_prtn_w)
            if i < 0:
                raise ValueError("Error while building buckets")

            if curr_prtn_w >= prtn_w: 
                # Actually need to split up weights in bucket, but for now we will
                # just utilize the fact that the bucket's weight is 1
                found_ij = True
                alias_table_paritions.append([1 for _ in range(partition_size)]) 
                spill = (curr_prtn_w) - prtn_w
                h_idx += j
                l_idx += i
            else:
                j += 1
                i = partition_size - j
        buckets_created += partition_size

    remaining = (len(h_weights) - (h_idx)) + (len(l_weights) - (l_idx))
    buckets_created += remaining
    # last_bucket_weight = sum(h_weights[h_idx:]) + sum(l_weights[l_idx:]) + spill
    # print(f"Last bucket weight: {last_bucket_weight}") 
    alias_table_paritions.append([1 for _ in range(remaining)]) 
    if buckets_created != n:
        print(buckets_created)
        raise ValueError("Number of buckets not equal to number of items")

    return alias_table_paritions

def main():
    n_partitions = 20
    fake_ids = range(210)
    fake_weights = [rd.randint(1,100) for _ in range(210)]
    alias_table = dist_alias_table(n_partitions, fake_ids, fake_weights)
    print("Bueno")