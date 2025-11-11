import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import requests._
import spark.implicits._
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.spark.sql.DataFrame

val apiUrl = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"

val response = get(apiUrl, headers = headers)
val total = response.text()
val dfFromText= spark.read.json(Seq(total).toDS)

// select the columns you want to include in the message
val messageDF = dfFromText.select($"id", $"stationName", $"lineName", $"towards", $"expectedArrival")

// convert the DataFrame to a JSON string
val messageJson = messageDF.toJSON.collect().mkString("\n")

// set up the Kafka producer properties
val props = new java.util.Properties()
props.put("bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
props.put("key.serializer", classOf[StringSerializer].getName)
props.put("value.serializer", classOf[StringSerializer].getName)

// create the Kafka producer
val producer = new KafkaProducerString, String

// send the message to the Kafka topic
val topic = "test_Raji"
val key = "1"
val record = new ProducerRecord[String, String](topic, key, messageJson)
producer.send(record)

// close the producer
producer.close()