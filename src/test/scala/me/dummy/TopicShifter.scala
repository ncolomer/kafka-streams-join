package me.dummy

import java.lang.Thread.UncaughtExceptionHandler

import org.apache.kafka.common.serialization.Serdes.ByteArraySerde
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, TopologyBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

class TopicShifter(val inputTopic: String,
                   val delay: Duration,
                   zookeeperConnect: String,
                   bootstrapServers: String) {

  private val log = LoggerFactory.getLogger(this.getClass)

  val outputTopic = s"$inputTopic.${delay.toString.replaceAll("(\\d+) (\\w).*", "$1$2")}"

  private val processor = new ProcessorSupplier[Array[Byte], Array[Byte]] {
    override def get() = new Processor[Array[Byte], Array[Byte]] {
      private var context: ProcessorContext = null
      override def init(context: ProcessorContext) = this.context = context
      override def close() = ()
      override def punctuate(timestamp: Long) = ()
      override def process(key: Array[Byte], value: Array[Byte]) = {
        val ts = context.timestamp()
        val now = System.currentTimeMillis
        val diff = ts - (now - delay.toMillis)
        if (diff > 0) Thread.sleep(diff)
        context.forward(key, value)
        context.commit()
      }
    }
  }

  val stream = {
    val config = new StreamsConfig(Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> s"$outputTopic-topic-shifter",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      StreamsConfig.ZOOKEEPER_CONNECT_CONFIG -> zookeeperConnect,
      StreamsConfig.KEY_SERDE_CLASS_CONFIG -> classOf[ByteArraySerde].getName,
      StreamsConfig.VALUE_SERDE_CLASS_CONFIG -> classOf[ByteArraySerde].getName
    ))
    val builder = new TopologyBuilder()
      .addSource("source", inputTopic)
      .addProcessor("processor", processor, "source")
      .addSink("sink", outputTopic, "processor")
    new KafkaStreams(builder, config)
  }

  stream.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable) =
      log.error(s"Error in topic shifter $outputTopic", e)
  })

}
