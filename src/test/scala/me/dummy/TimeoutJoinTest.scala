package me.dummy

import java.util.UUID

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.scalatest.Matchers._
import org.scalatest.{Outcome, fixture}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

class TimeoutJoinTest extends fixture.FunSuite {

  val Window = 1.second

  val Ser = new StringSerializer
  val Des = new StringDeserializer

  class Topics {
    val uuid = UUID.randomUUID.toString
    val DisplayTopic = s"$uuid-display"
    val DisplayShiftedTopic = s"$uuid-display.${Window.toString.replaceAll("(\\d+) (\\w).*", "$1$2")}"
    val ClickTopic = s"$uuid-click"
    val ClickedDisplayTopic = s"$uuid-clicked-display"
    val MissedDisplayTopic = s"$uuid-missed-display"
  }

  override type FixtureParam = Topics
  override protected def withFixture(test: OneArgTest): Outcome = {
    val topics = new Topics
    KafkaUtils.createTopic(topics.DisplayTopic)
    KafkaUtils.createTopic(topics.DisplayShiftedTopic)
    KafkaUtils.createTopic(topics.ClickTopic)
    KafkaUtils.createTopic(topics.ClickedDisplayTopic)
    KafkaUtils.createTopic(topics.MissedDisplayTopic)
    
    val streams = buildStreams(topics)
    streams.foreach(_.cleanUp())
    streams.foreach(_.start())
    Thread.sleep(2000) // Let Kafka Streams boot up
    
    try {
      withFixture(test.toNoArgTest(topics))
    } finally {
      streams.foreach(_.close())
      Thread.sleep(2000) // Let Kafka Streams tear down
    }
  }

  def buildStreams(topics: Topics): List[KafkaStreams] = {

    val shifter = new TopicShifter(topics.DisplayTopic, Window, KafkaUtils.zookeeperConnect(), KafkaUtils.bootstrapServers()).stream

    val builder = new KStreamBuilder

    val displayStream = builder.stream[String, String](topics.DisplayTopic)
    val displayShiftedStream = builder.stream[String, String](topics.DisplayShiftedTopic)
    val clickStream = builder.stream[String, String](topics.ClickTopic)
    val clickedDisplayStream = builder.stream[String, String](topics.ClickedDisplayTopic)
    val missedDisplayStream = builder.stream[String, String](topics.MissedDisplayTopic)

    def action(name: String) = new ForeachAction[String, String] {
      override def apply(key: String, value: String): Unit = println(s"$name: ${key -> value}")
    }
    displayStream.foreach(action(topics.DisplayTopic))
    displayShiftedStream.foreach(action(topics.DisplayShiftedTopic))
    clickStream.foreach(action(topics.ClickTopic))
    clickedDisplayStream.foreach(action(topics.ClickedDisplayTopic))
    missedDisplayStream.foreach(action(topics.MissedDisplayTopic))

    // Emit click events with their original display (if a join occurred in the observation window)
    clickStream
      .join(displayStream, new ValueJoiner[String, String, String] {
        override def apply(value1: String, value2: String): String = s"""{"display":$value2,"click":$value1}"""
      }, JoinWindows.of("occurred-before").before(Window.toMillis))
      .to(Serdes.String, Serdes.String, topics.ClickedDisplayTopic)

    // Emit non-click events
    displayShiftedStream
      .leftJoin(clickStream, new ValueJoiner[String, String, (String, Option[String])] {
        override def apply(value1: String, value2: String): (String, Option[String]) = (value1, Option(value2))
      }, JoinWindows.of("occurred-after").after(Window.toMillis))
      .filter(new Predicate[String, (String, Option[String])] {
        override def test(key: String, value: (String, Option[String])): Boolean = value._2.isEmpty
      })
      .mapValues(new ValueMapper[(String, Option[String]), String] {
        override def apply(value: (String, Option[String])): String = value._1
      })
      .to(Serdes.String, Serdes.String, topics.MissedDisplayTopic)

    val app = new KafkaStreams(builder, new StreamsConfig(Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> "time-join-test",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> KafkaUtils.bootstrapServers(),
      StreamsConfig.ZOOKEEPER_CONNECT_CONFIG -> KafkaUtils.zookeeperConnect(),
      StreamsConfig.KEY_SERDE_CLASS_CONFIG -> classOf[StringSerde].getName,
      StreamsConfig.VALUE_SERDE_CLASS_CONFIG -> classOf[StringSerde].getName
    )))

    List(shifter, app)

  }

  test("emit display and click in window, should output join") { topics =>
    // When
    KafkaUtils.writeToTopic(topics.DisplayTopic, Ser, Ser, List(("0f1f53a0-44f5-4b84-9699-fe853c90ed1c", """{"type":"display"}""")))
    assume(KafkaUtils.readFromTopic(topics.DisplayTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.DisplayTopic} not ready")

    Thread.sleep(Window.toMillis / 2)
    KafkaUtils.writeToTopic(topics.ClickTopic, Ser, Ser, List(("0f1f53a0-44f5-4b84-9699-fe853c90ed1c", """{"type":"click"}""")))
    assume(KafkaUtils.readFromTopic(topics.ClickTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.ClickTopic} not ready")

    Thread.sleep(Window.toMillis)
    assume(KafkaUtils.readFromTopic(topics.DisplayShiftedTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.DisplayShiftedTopic} not ready")

    // Then
    val clickedDisplays = KafkaUtils.readFromTopic(topics.ClickedDisplayTopic, Des, Des, expect = Some(1))
    clickedDisplays should have size 1
    clickedDisplays.map(kv => kv.key() -> kv.value()) should contain("0f1f53a0-44f5-4b84-9699-fe853c90ed1c" -> """{"display":{"type":"display"},"click":{"type":"click"}}""")

    val notClickedDisplays = KafkaUtils.readFromTopic(topics.MissedDisplayTopic, Des, Des, expect = None)
    notClickedDisplays shouldBe empty
  }

  test("emit display and click out window, should output no-join") { topics =>
    // When
    KafkaUtils.writeToTopic(topics.DisplayTopic, Ser, Ser, List(("0f1f53a0-44f5-4b84-9699-fe853c90ed1c", """{"type":"display"}""")))
    assume(KafkaUtils.readFromTopic(topics.DisplayTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.DisplayTopic} not ready")

    Thread.sleep(Window.toMillis * 2)
    KafkaUtils.writeToTopic(topics.ClickTopic, Ser, Ser, List(("0f1f53a0-44f5-4b84-9699-fe853c90ed1c", """{"type":"click"}""")))
    assume(KafkaUtils.readFromTopic(topics.ClickTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.ClickTopic} not ready")

    assume(KafkaUtils.readFromTopic(topics.DisplayShiftedTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.DisplayShiftedTopic} not ready")

    // Then
    val clickedDisplays = KafkaUtils.readFromTopic(topics.ClickedDisplayTopic, Des, Des, expect = Some(1))
    clickedDisplays shouldBe empty

    val notClickedDisplays = KafkaUtils.readFromTopic(topics.MissedDisplayTopic, Des, Des, expect = None)
    notClickedDisplays.map(kv => kv.key() -> kv.value()) should contain("0f1f53a0-44f5-4b84-9699-fe853c90ed1c" -> """{"type":"display"}""")
  }

  test("emit display and non related click, should output no-join") { topics =>
    // When
    KafkaUtils.writeToTopic(topics.DisplayTopic, Ser, Ser, List(("0f1f53a0-44f5-4b84-9699-fe853c90ed1c", """{"type":"display"}""")))
    assume(KafkaUtils.readFromTopic(topics.DisplayTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.DisplayTopic} not ready")

    Thread.sleep(Window.toMillis / 2)
    KafkaUtils.writeToTopic(topics.ClickTopic, Ser, Ser, List(("9750c569-44c2-49e6-854e-01e0eae04bb6", """{"type":"click"}""")))
    assume(KafkaUtils.readFromTopic(topics.ClickTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.ClickTopic} not ready")

    Thread.sleep(Window.toMillis)
    assume(KafkaUtils.readFromTopic(topics.DisplayShiftedTopic, Des, Des, expect = Some(1)).size == 1, s"${topics.DisplayShiftedTopic} not ready")

    // Then
    val clickedDisplays = KafkaUtils.readFromTopic(topics.ClickedDisplayTopic, Des, Des, expect = Some(1))
    clickedDisplays shouldBe empty

    val notClickedDisplays = KafkaUtils.readFromTopic(topics.MissedDisplayTopic, Des, Des, expect = None)
    notClickedDisplays.map(kv => kv.key() -> kv.value()) should contain("0f1f53a0-44f5-4b84-9699-fe853c90ed1c" -> """{"type":"display"}""")
  }

}
