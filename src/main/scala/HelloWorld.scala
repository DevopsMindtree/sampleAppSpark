import org.apache.spark.sql.SparkSession



  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  object HelloWorld {

    def main(args:Array[String]): Unit = {


      val spark = SparkSession.builder.appName("Structured socket").getOrCreate()

      import spark.implicits._

      val lines = spark.readStream.format("socket").option("host", "localhost").option("port",777).load()

      lines.isStreaming

      lines.printSchema()

      val words = lines.as[String].flatMap(_.split(" "))

      val wordCount = words.groupBy("value").count()

      val query = wordCount.writeStream.outputMode("complete").format("console").start()

      query.awaitTermination()

    }

}
