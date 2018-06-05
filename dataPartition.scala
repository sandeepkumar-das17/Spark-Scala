import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession  
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.mutable.Map
import scala.collection.{ Iterator, mutable }
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.io.File

object dataPartition {
  def main(args: Array[String]) {
    new PartitionerD().mainProcessor()
  }
}

class PartitionerD extends Serializable {
  
  //var warehouse:String = s""
  
  def mainProcessor():Unit = {
    
    
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    
    val spark = SparkSession.builder().appName("Data Partitioner").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

    

    import spark.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
  
    import org.apache.spark.sql.types._
    
    spark.conf.set("hive.exec.orc.split.strategy","ETL" )
    spark.conf.set("spark.debug.maxToStringFields",100 )
    spark.conf.set("spark.executor.cores",4)
    spark.conf.set("spark.driver.cores",4)
    spark.conf.set("spark.yarn.executor.memoryOverhead",4)
    spark.conf.set("spark.yarn.driver.memoryOverhead", 8192)

    spark.conf.set("spark.executor.memory", "4g")
    spark.conf.set("spark.driver.memory", "4g")
            
    val column_name = spark.conf.get("spark.column_name")
    val table_name=spark.conf.get("spark.table_name")
    val target_dir=spark.conf.get("spark.target_path")
    val query_string = "select "+ column_name +" as "+ column_name +"_split, * from " + table_name 
    System.out.println("Executing :"+query_string)
    val df1 = spark.sql(query_string)
    df1.printSchema()
    df1.coalesce(1).write.partitionBy(column_name+"_split").mode("Overwrite").option("header", "true").option("delimiter", "|").format("csv").save(target_dir)
  }
  
}
