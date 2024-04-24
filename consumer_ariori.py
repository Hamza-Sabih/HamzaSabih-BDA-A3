from kafka import KafkaConsumer
import pandas as pd
from itertools import combinations

class AprioriAlgorithm:
    def __init__(self, min_sup):
        self.min_sup = min_sup
        self.df = pd.DataFrame()
    
    def calc_sup(self, itemset):
        if not itemset:
            return 0
        itemset_count = self.df[list(itemset)].all(axis=1).sum()
        return itemset_count / len(self.df)

    def gen_candidates(self, prev_itemsets, k):
        return set([i.union(j) for i in prev_itemsets for j in prev_itemsets if len(i.union(j)) == k])

    def apriori(self):
        freq_itemsets_1 = {frozenset([item]) for item in self.df.columns if self.calc_sup([item]) >= self.min_sup}
        frequent_itemsets = [freq_itemsets_1]
        k = 2
        while True:
            candidates = self.gen_candidates(frequent_itemsets[k-2], k)
            freq_itemsets_k = set()
            for candidate in candidates:
                supp = self.calc_sup(candidate)
                if supp >= self.min_sup:
                    freq_itemsets_k.add(candidate)
                    print(f"Frequent itemset of size {k}: {set(candidate)} with support {supp:.4f}")
            if not freq_itemsets_k:
                break
            frequent_itemsets.append(freq_itemsets_k)
            k += 1
        return frequent_itemsets

    def consume_transactions(self, topic_name, bootstrap_servers):
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='apriori-group'
        )

        data = []
        for message in consumer:
            transaction = eval(message.value.decode('utf-8'))
            data.append(transaction)
            self.df = pd.DataFrame(data)
            if len(data) % 100 == 0:
                print("Executing Apriori on the latest batch of transactions...")
                frequent_itemsets = self.apriori()
                print("Updated frequent itemsets:", frequent_itemsets)

# Initialize the AprioriAlgorithm instance with minimum support
apriori_instance = AprioriAlgorithm(min_sup=0.01)

# Start consuming transactions from Kafka topic
apriori_instance.consume_transactions(topic_name='preprocessed_data_topic', bootstrap_servers=['localhost:9092'])






