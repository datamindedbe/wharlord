package be.dataminded.wharlord.test

import org.apache.spark.sql.SparkSession

trait SparkContexts {

  protected val spark: SparkSession =
    SparkSession.builder
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "5")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .appName("Testing")
      .getOrCreate()

  def stopSession(): Unit = {
    spark.stop()
  }

  sys.addShutdownHook(stopSession())
}
