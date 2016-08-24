package me.dummy

import java.util.{Properties, UUID}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, _}


object KafkaUtils {

  val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Get this broker connect string.
    */
  def bootstrapServers() = "localhost:9092"

  /**
    * Get this broker's zookeeper connect string.
    */
  def zookeeperConnect() = "localhost:2181"

  /**
    * Create a Kafka topic with the given parameters.
    */
  def createTopic(topic: String, partitions: Int = 1, replication: Int = 1, topicConfig: Properties = new Properties()) = {
    log.debug(s"Creating topic { name: $topic, partitions: $partitions, replication: $replication, config: $topicConfig }")
    val zkClient = new ZkClient(zookeeperConnect(),
      10 * 1000, // DEFAULT_ZK_SESSION_TIMEOUT_MS,
      8 * 1000, // DEFAULT_ZK_CONNECTION_TIMEOUT_MS
      new ZkSerializer {
        def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")
        def deserialize(bytes: Array[Byte]): Object = if (bytes == null) null else new String(bytes, "UTF-8")
      })
    val zkUtils = new ZkUtils(zkClient, new ZkConnection("127.0.0.0"), false)
    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced)
    zkClient.close()
  }

  def writeToTopic[K, V](topic: String,
                         keySerializer: Serializer[K],
                         valSerializer: Serializer[V],
                         records: List[(K, V)]): Unit =  {
    val producer: Producer[K, V] = new KafkaProducer[K, V]({
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass)
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSerializer.getClass)
      p
    })
    records
      .map({case (key , value) => new ProducerRecord[K, V](topic, key, value)})
      .foreach(producer.send)
    producer.flush()
    producer.close()
  }

  def readFromTopic[K, V](topic: String,
                          keyDeserializer: Deserializer[K],
                          valDeserializer: Deserializer[V],
                          expect: Option[Int] = None,
                          wait: Option[Duration] = Some(1.second)): List[ConsumerRecord[K, V]] =  {
    val consumer  = new KafkaConsumer[K, V]({
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID.toString)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass)
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeserializer.getClass)
      p
    })
    consumer.subscribe(List(topic))
    val end = wait.map(_.toMillis + System.currentTimeMillis)
    var records: ConsumerRecords[K, V] = null
    val accumulator = ListBuffer.empty[ConsumerRecord[K, V]]
    def hasMoreToFetch = expect.forall(accumulator.size < _)
    def hasNotTimeout = end.exists(_ > System.currentTimeMillis)
    val pollRecords = () => { records = consumer.poll(100); true }
    while (hasMoreToFetch && hasNotTimeout && pollRecords()) accumulator ++= records
    consumer.close()
    accumulator.toList
  }

}
