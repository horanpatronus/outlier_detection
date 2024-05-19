# outlier_detection

Tutorial jalanin program:
1. Jalanin Kafka
   ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
   ./bin/kafka-server-start.sh ./config/server.properties

2. buat topic (cukup sekali di awal)
   ~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic transactions
   ~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic results
   ~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic anomalies 
   
3. Jalanin topic
   ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning
   ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic results --from-beginning
   ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic anomalies --from-beginning

4. Jalanin producer dan outlier detector
   a) masuk ke folder streaming
      cd streaming
   b) jalanin outlier detector
      python kafka.py
   c) jalanin producer
      python producer.py
