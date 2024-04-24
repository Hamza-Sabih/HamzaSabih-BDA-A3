## Streaming Data Insights with Kafka and Frequent Itemset Mining

### Introduction
This project showcases a streaming data analysis pipeline using Kafka and frequent itemset mining algorithms. The pipeline includes a producer application that streams preprocessed data to a Kafka topic and three consumer applications that subscribe to the data stream. The consumers implement the Apriori algorithm, the PCY (Park-Chen-Yu) algorithm, and a custom analysis approach to extract insights from the streaming data.

### Approach
1. *Kafka Integration*: We chose Kafka due to its distributed, fault-tolerant, and scalable nature, making it ideal for real-time data streaming applications.

2. *Frequent Itemset Mining Algorithms*:
    - *Apriori Algorithm*: Implemented in one consumer, this algorithm is a classic approach for finding frequent itemsets. It calculates support counts for each item and generates frequent itemsets based on a minimum support count threshold.
    - *PCY Algorithm*: Implemented in another consumer, the PCY algorithm is a variation of Apriori that reduces memory usage by using a hash-based technique. It efficiently identifies frequent itemsets while maintaining performance.
    - *Custom Analysis*: The third consumer performs a custom analysis using innovative techniques beyond basic frequent itemset mining. This could include sliding window analysis, approximation techniques, or other advanced algorithms tailored to the specific dataset.

### Why Kafka and Frequent Itemset Mining?
- *Real-Time Analysis*: Kafka enables real-time data processing, crucial for streaming applications where data is continuously generated.
- *Scalability*: Kafka's distributed architecture allows horizontal scaling to handle large volumes of data.
- *Frequent Itemset Mining*: These algorithms provide insights into patterns and associations within the data, valuable for market basket analysis, recommendation systems, and more.

### Group Members
- *Shahroze Naveed (22i-1922)*
- *Hamza Sabhi (22i-1948)*
- *Taha Rasheed (22i-2009)*

### Conclusion
This project demonstrates the power of Kafka for building scalable, real-time streaming data pipelines, combined with frequent itemset mining algorithms for extracting meaningful insights from the data stream. The combination of Kafka and these algorithms offers a robust solution for analyzing large-scale streaming data in various domains.
