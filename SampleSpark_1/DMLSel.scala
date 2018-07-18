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
    
    ---- emp.csv ----
    Sabir Ahmed,110,Tech Lead,GE101,Bangalore,02-Oct-2009,Manufacturing
    Rohini Shukla,114,Software Engineer,GE101,Bangalore,17-Jun-2015,Manufacturing
    Amit Kumar,167,Tech Analyst,CISCO02,Bangalore,14-Nov-2013,Prod Eng
    Ankit Mishra,104,Tech Lead,GE101,Bangalore,11-Jun-2009,Manufacturing
    Babita Khuntia,149,Tech Analyst,AMEX1,Bangalore,16-Aug-2012,Banking and Financials
    Debangan Ghosh,112,Software Engineer,AMEX1,Bangalore,17-Jun-2015,Banking and Financials
    Subashish Mishra,101,Project Lead,AMEX1,Bangalore,08-Mar-2009,Banking and Financials
    Ranjit Kumar,146,Tech Analyst,GE101,Bangalore,13-Jul-2008,Manufacturing

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
		
    ---- proj.csv -----
    GE101,Analytics,TNM,General Electricals,Manufacturing
    AMEX1,BDPilot,POC,American Express,Banking and Financials
    CISCO02,CAR,TNM,Cisco,Prod Eng
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
