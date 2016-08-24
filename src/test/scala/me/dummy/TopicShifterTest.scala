package me.dummy

import java.util.UUID

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.concurrent.duration._

class TopicShifterTest extends FunSuite {

  test("should delay records") {
    // Given
    val delay = 1.second
    val uuid = UUID.randomUUID.toString
    val TopicName = s"$uuid-topic"
    val DelayedTopicName = s"$uuid-topic.${delay.toString.replaceAll("(\\d+) (\\w).*", "$1$2")}"

    KafkaUtils.createTopic(TopicName)
    KafkaUtils.createTopic(DelayedTopicName)

    val stream = new TopicShifter(TopicName, delay, KafkaUtils.zookeeperConnect(), KafkaUtils.bootstrapServers()).stream

    stream.start()
    Thread.sleep(1000) // Let Kafka Streams boot up

    // When
    KafkaUtils.writeToTopic(TopicName, new StringSerializer, new StringSerializer, List(("0f1f53a0-44f5-4b84-9699-fe853c90ed1c", """{"type":"display"}""")))
    assume(KafkaUtils.readFromTopic(TopicName, new StringDeserializer, new StringDeserializer, expect = Some(1)).size == 1)

    // Then
    Thread.sleep(delay.toMillis / 2)
    KafkaUtils.readFromTopic(DelayedTopicName, new StringDeserializer, new StringDeserializer, wait = None) shouldBe empty

    Thread.sleep(delay.toMillis)
    val records = KafkaUtils.readFromTopic(DelayedTopicName, new StringDeserializer, new StringDeserializer, expect = Some(1))
    records should have size 1
    records.map(kv => kv.key() -> kv.value()) should contain("0f1f53a0-44f5-4b84-9699-fe853c90ed1c" -> """{"type":"display"}""")

    stream.close()

  }

}
