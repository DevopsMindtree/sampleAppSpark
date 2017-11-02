import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object sampleApp {

  def main(args:Array[String]): Unit ={

  //val conf = new SparkConf()

    val spark = SparkSession
      .builder
      .appName("Sample APP")
      .getOrCreate()

      //  val sc = new SparkContext(conf)

    val data = List("marriott hotel", "hotel marriott")

    spark.sparkContext.parallelize(data).flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x+y).foreach(println)

      //
    
  }

}
