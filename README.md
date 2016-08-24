# kafka-streams-join

## How to run?

- Ensure you have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html),
[Scala 2.11](http://www.scala-lang.org/download/),
[SBT](http://www.scala-sbt.org/download.html) and the latest
[Confluent platform](http://www.confluent.io/download) installed

- [Start the Kafka cluster](http://docs.confluent.io/3.0.0/streams/quickstart.html#start-the-kafka-cluster),
Zookeeper must be reachable at `localhost:2181` and Kafka broker at `localhost:9092`

- Run tests using `sbt clean test`

## Referring topics on the [confluent-platform](https://groups.google.com/forum/#!forum/confluent-platform) user group

- [Process a Kafka topic with a delay using Kafka Stream](https://groups.google.com/forum/#!topic/confluent-platform/rn8CJu7Wfcw)
- [[Kafka Streams] Detect absence of join after a time period](https://groups.google.com/forum/#!topic/confluent-platform/KcKi54HZZdQ)