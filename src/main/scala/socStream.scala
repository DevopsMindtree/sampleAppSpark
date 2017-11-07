import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object socStream {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf()
    conf.setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))

    val inputDStream = ssc.socketTextStream("34.253.181.64", 7777)

    val words = inputDStream.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
