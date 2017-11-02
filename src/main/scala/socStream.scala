import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object socStream {

  def main(args:Array[String]): Unit ={



    val conf = new SparkConf()
    conf.setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))

    val inputDStream = ssc.socketTextStream("34.253.181.64", 7777)
    val mappedDStream = inputDStream.map{record => record+"...."}

    mappedDStream.print()
    inputDStream.foreachRDD{

      rdd =>

        val mappedRdd = rdd.map(record => record+"....")
        val filterRdd = mappedRdd.filter(record => record.contains("ERROR"))

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
