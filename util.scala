import org.apache.spark.sql.functions._

object util extends Serializable {
  
  val stb_dervBillingStatus: ((String) => String) = (project_type: String) => {
      System.out.println("project_type: "+project_type.trim())
      if(project_type.trim().equals("POC"))
        s"Non-Billable"
      else 
        s"Billable"
  }
  
  val dervBillingStatus = udf(stb_dervBillingStatus)
  
}
