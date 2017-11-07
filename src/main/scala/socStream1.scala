import AvroParser.KafkaMessage
import org.apache.spark.sql.functions.from_json
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp



object socStream1 {
  case class Person(name: String, age: Integer)
  def main(args:Array[String]): Unit ={

    val schemaString = """{
                            "type": "record",
                            "name": "Reservations",
                            "fields": [
                              { "name": "EventSource", "type": "string" },
                              { "name": "KeysOfInterest", "type": "string" },
                              { "name": "EventIdentifier", "type": "string" },
                              { "name": "TimeOfEvent", "type": "string" },
                              { "name": "EventType", "type": "string" },
                              { "name": "Body", "type": "string" }
                            ]
                       }"""


    val messageSchema = new Schema.Parser().parse(schemaString)
    val reader = new GenericDatumReader[GenericRecord](messageSchema)

    // Register implicit encoder for map operation
    implicit val encoder: Encoder[GenericRecord] = org.apache.spark.sql.Encoders.kryo[GenericRecord]

    val spark = SparkSession
      .builder()
      .appName("Kafka AvroMessage Streaming")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()
      .as[KafkaMessage]
      .select($"value".as[Array[Byte]])
      .map(msg => {
        //if(!d.iterator.isEmpty)//while(d.iterator.hasNext)

        //val r = it.iterator.next()
        //val decoder: Decoder = DecoderFactory.get(). binaryDecoder(r, null)
        //val userData: GenericRecord = reader.read(null, decoder)

        // d.iterator[]
        //if(d.length > 0)
        {
          val decoder: Decoder = DecoderFactory.get().binaryDecoder(msg, null)
          val userData: GenericRecord = reader1.read(null, decoder)

          //val kt = userData.get("Key").asInstanceOf[Int]
          val content = userData.get("Value").asInstanceOf[org.apache.avro.util.Utf8].getBytes


          val decoder1: Decoder = DecoderFactory.get().binaryDecoder(content, null)
          val userData1: GenericRecord = reader.read(null, decoder1)

          val et = userData1.get("EventType").asInstanceOf[org.apache.avro.util.Utf8].toString
          val es = userData1.get("EventSource").asInstanceOf[org.apache.avro.util.Utf8].toString
          val koi = userData1.get("KeysOfInterest").asInstanceOf[org.apache.avro.util.Utf8].toString
          val ei = userData1.get("EventIdentifier").asInstanceOf[org.apache.avro.util.Utf8].toString
          val toi = userData1.get("TimeOfEvent").asInstanceOf[org.apache.avro.util.Utf8].toString
          val body = userData1.get("Body").asInstanceOf[org.apache.avro.util.Utf8].toString

          et + ":" + es + ":" + ":" + koi + ":" + body
        }
      }
      )

    ds1.printSchema()

    val query = ds1.writeStream
      .outputMode("append")
      .queryName("table")
      .format("console")
      .start()

    query.awaitTermination()




  }

}
