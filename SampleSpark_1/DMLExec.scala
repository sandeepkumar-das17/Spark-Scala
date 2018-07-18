import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession  
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.mutable.Map
import scala.collection.{ Iterator, mutable }
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.io.File

object DMLExec {
  def main(args: Array[String]) {
    new ExecuteOp().mainProcessor()
  }
}

class ExecuteOp extends Serializable {
  
  //var warehouse:String = s""
  
  def mainProcessor():Unit = {
    
    
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    
    val spark = SparkSession.builder().appName("DML Exec").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

    val SEL = DMLSel
    val utl = util
    

    import spark.implicits._
    import org.apache.spark.sql.Row
    
  
    import org.apache.spark.sql.types._
    
    
    
    cookUpData(spark)
  
    
    //Following settings overrides the configuration values given in the conf file
    spark.conf.set("hive.exec.orc.split.strategy","ETL" )
    spark.conf.set("spark.debug.maxToStringFields",100 )
    spark.conf.set("spark.executor.cores",2)
    spark.conf.set("spark.driver.cores",1)
    spark.conf.set("spark.yarn.executor.memoryOverhead",4)
    spark.conf.set("spark.yarn.driver.memoryOverhead", 8192)

    spark.conf.set("spark.executor.memory", "1g")
    spark.conf.set("spark.driver.memory", "1g")         
    
    //val column_name = spark.conf.get("spark.column_name")
    //val table_name=spark.conf.get("spark.table_name")
    //val target_dir=spark.conf.get("spark.target_path")
    
    //Select the employee data
    System.out.println("Executing :"+SEL.sel_emp)
    val emp_df = spark.sql(SEL.sel_emp)
    emp_df.collect.foreach(println)
    emp_df.createOrReplaceTempView("empTbl")
    
    
    //Select the project data
    System.out.println("Executing :"+SEL.sel_proj)
    val proj_df = spark.sql(SEL.sel_proj)  
    proj_df.collect.foreach(println)
    proj_df.createOrReplaceTempView("projTbl")
    
    
    //Find the billable employees
    val billable_emp_df = spark.sql(SEL.empJoinproj)
    val billable_emp_df1= billable_emp_df.withColumn(s"billing_status", utl.dervBillingStatus(billable_emp_df(s"project_type")))
    billable_emp_df1.collect.foreach(println)
    billable_emp_df1.coalesce(1).write.mode("Overwrite").option("header", "true").format("csv").save("./resources/billing.data")
    
    //billable_emp_df.printSchema()
    //billable_emp_df.collect.foreach(println)
    //billable_emp_df.coalesce(1).write.mode("Overwrite").option("header", "true").format("csv").save("./resources/billing.data")
    
    
  }
  
  def cookUpData(spark:SparkSession):Unit = {
    
   
    //Cook up the table data
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
    
    val empRDD = spark.sparkContext.textFile("./resources/emp.csv")
    val empschemaString = "name	id	designation	project_id	location	doj	dept_id"
    val empfields = empschemaString.split("\t").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val empschema = StructType(empfields)
    val emprowRDD = empRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), attributes(6).trim))
    val empDF = spark.createDataFrame(emprowRDD, empschema)
    empDF.printSchema()
    empDF.collect.foreach(println)
    empDF.createOrReplaceTempView("employee")
    
    val projRDD = spark.sparkContext.textFile("./resources/proj.csv")
    val schemaString = "project_id	project_name	project_type	customer_name	dept_id"
    val fields = schemaString.split("\t").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = projRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4).trim))
    val projDF = spark.createDataFrame(rowRDD, schema)
    projDF.printSchema()
    projDF.collect.foreach(println)
    projDF.createOrReplaceTempView("project")
  }
  
}
