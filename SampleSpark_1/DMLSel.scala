object DMLSel extends Serializable {
  
  /*Table definition
    create table if not exists employee
    (
    name string,
    id int,
    designation string,
    project_id string, 
    location string,
    doj string
    )
    partition by (dept_id string) 
    row format delimited fields terminated by '|'
    stored as sequencefile;
    */
  
  val sel_emp = "select name,id,designation,project_id,location,doj,dept_id from employee"
  
  /*
    create table if not exists project
    (
    project_id string, 
    project_name string,
    project_type string,
    customer_name string
    ) 
    partition by (dept_id string) 
    row format delimited fields terminated by '|'
    stored as sequencefile;
		*/
  
  val sel_proj = "select project_id,project_name,project_type,customer_name,dept_id from project"
  
  val empJoinproj="select "+
    "e.name,"+
    "e.designation,"+
    "e.project_id,"+
    "p.project_name,"+
    //"case when p.project_type = 'POC' then 'Non-Billable' else 'Billable' end as billing_status,p.project_type "+
    "p.project_type, "+
    "p.customer_name "+
    "from empTbl e left outer join projTbl p on (e.dept_id=p.dept_id and e.project_id=p.project_id) "
  
}
