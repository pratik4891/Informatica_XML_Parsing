import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import os
from glob import glob
import csv
import xlwt
import shutil
from shutil import copyfile


spark = SparkSession.builder.getOrCreate()
'''
def create_Excel(path,output_file):
    print("Input path is " + path)
    print("Output File name " + output_file )
    wb = xlwt.Workbook()
    for csvfile in glob(os.path.join(path, '*.csv')):
      
      fpath = csvfile.split("/", 1)
      print ("Current Path " + csvfile )
      fname = fpath[1].split("/", 4)
      print ("Current Path " + fname[4].split(".",1)[0] )
       
      
      ws = wb.add_sheet( fname[4].split(".",1)[0] )
      with open(csvfile, 'rb') as f:
        reader = csv.reader(f)
        for r, row in enumerate(reader):
            for c, col in enumerate(row):
                ws.write(r, c, col)
      wb.save(output_file)
'''

def create_Excel(path,output_file):
    print("Input path is " + path)
    print("Output File name " + output_file )
    wb = xlwt.Workbook()
    os.chdir(path)
    print ("Current Path " + os.getcwd())
    for file in glob("*.csv"):
      print ("Current Path " + os.path.splitext(file)[0] )
      ws = wb.add_sheet( os.path.splitext(file)[0] )
      with open(file, 'rb') as f:
        reader = csv.reader(f)
        for r, row in enumerate(reader):
            for c, col in enumerate(row):
                ws.write(r, c, col)
      wb.save(output_file)

def rename(path,name):
    print ("Current Path - " + path )
    f = glob(os.path.join(path,"part*.csv"))[0]
    os.rename(f, os.path.join(path,name))

def has_column(df, col):
    column = '"' + col + '"'
    print column
    if col in df.columns:
        print ("Required Transformation found...")
        return True
    else:
        print ("Required Transformation not found...")
        return False


if __name__ == "__main__":
  print("Hello")
  sourcePath1 = sys.argv[1]
  sourcePath="file://" + sourcePath1
  output1 = sys.argv[2]
  output  = "file://" + output1
  output_transformation=output+"/"+"Transformation"
  output_write=output1+"/"+"Transformation"
  object_name = sys.argv[3]
  wkf_path= "file://" + sys.argv[4]

  spark = SparkSession \
        .builder \
        .appName("PySpark Flatten XML Structure ") \
        .master("local[*]") \
        .config("spark.jars", "file:///usr/local/spark/spark/jars/spark-xml_2.12-0.9.0.jar") \
        .config("spark.executor.extraClassPath", "file:///data2/informatica/rudra/spark/spark-3.0.0-preview2-bin-hadoop2.7/jars/spark-xml_2.12-0.9.0.jar") \
        .config("spark.executor.extraLibrary", "file:///data2/informatica/rudra/spark/spark-3.0.0-preview2-bin-hadoop2.7/jars/spark-xml_2.12-0.9.0.jar") \
        .config("spark.driver.extraClassPath", "file:///data2/informatica/rudra/spark/spark-3.0.0-preview2-bin-hadoop2.7/jars/spark-xml_2.12-0.9.0.jar") \
        .getOrCreate()

  
  print ("Get Workflow details ....")

  df_wf=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "SESSIONEXTENSION").load(wkf_path)
  df_wf.select(col("_SINSTANCENAME").alias("Obejct_Name") , col("_TYPE").alias("Obejct_Type") , col("CONNECTIONREFERENCE._CONNECTIONNAME").alias("Connection_Name") , col("CONNECTIONREFERENCE._CONNECTIONSUBTYPE").alias("Connection_Subtype")  ).filter("Connection_Subtype is not null ").createOrReplaceTempView("df_wf_con")

  print ("Get transformation details .......")
 
  df_map_transformation=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "MAPPING").load(sourcePath)
  df_map_transformation.select (col("_NAME").alias("Mapping_Name") , explode(col("INSTANCE")  )).select("Mapping_Name" , col("col").cast(" struct<ASSOCIATED_SOURCE_INSTANCE:struct<NAME:string,VALUE:string>,TABLEATTRIBUTE:array<struct<NAME:string,VALUE:string>>,DBDNAME:string,DESCRIPTION:string,NAME:string,REUSABLE:string,TRANSFORMATION_NAME:string,TRANSFORMATION_TYPE:string,TYPE:string,VALUE:string>  "  )  ).select ("Mapping_Name" , col("col.NAME") , col("col.TRANSFORMATION_TYPE")  ).createOrReplaceTempView("df_map_transformation")

  df_connector=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "CONNECTOR").load(sourcePath)
  df_connector.select( col("_FROMINSTANCE").alias("From Transformation") , col("_FROMINSTANCETYPE").alias("From Type") , col("_FROMFIELD").alias("From Field") , col("_TOINSTANCE").alias("To Transformation") , col("_TOINSTANCETYPE").alias("To Type") , col("_TOFIELD").alias("To field")  ).createOrReplaceTempView("df_connector")
  df_transformation=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "TRANSFORMATION").load(sourcePath)
  df_transformation.select(col("_TYPE").alias("Transformation_Type") , col("_NAME").alias("Transformation_Name"),explode(arrays_zip(col("TRANSFORMFIELD._DATATYPE") ,col("TRANSFORMFIELD._NAME") ,col("TRANSFORMFIELD._EXPRESSION") , col("TRANSFORMFIELD._PRECISION") , col("TRANSFORMFIELD._SCALE") , col("TRANSFORMFIELD._PORTTYPE"), col("TRANSFORMFIELD._GROUP")  )) ).select("Transformation_Name" , "Transformation_Type" , col("col").cast("struct<DATATYPE:string,NAME:string,EXPRESSION:string,PRECISION:bigint,SCALE:bigint,PORTTYPE:string , GROUP:string>" )).select("Transformation_Name" , "Transformation_Type" ,col("col.DATATYPE").alias("Column_DATATYPE") , col("col.NAME").alias("Column_NAME"),col("col.EXPRESSION").alias("Column_EXPRESSION") , col("col.PRECISION").alias("Column_PRECISION") , col("col.SCALE").alias("Column_SCALE") , col("col.PORTTYPE").alias("Column_PORTTYPE") , col("col.GROUP").alias("Column_GROUP")   ).createOrReplaceTempView("df_transformation")

  spark.sql(" select distinct A.`From Transformation` , A.`From Type` ,B.Column_GROUP as `From Group` , A.`To Transformation` , A.`To Type` , C.Column_GROUP as `To Group`   from df_connector A left outer join df_transformation B on A.`From Transformation` = B.Transformation_Name and A.`From Type`=B.Transformation_Type and A.`From Field` = B.Column_NAME  left outer join df_transformation C on A.`To Transformation` = C.Transformation_Name and A.`To Type`=C.Transformation_Type and A.`To Field` = C.Column_NAME     ").createOrReplaceTempView("df_con_group")
  spark.sql("select * from df_con_group where `To Type` <> 'Source Qualifier' ").show()
 


  df_src1=df_transformation.filter(" _TYPE like '%Source%' ").select (col("_NAME").alias("src"))
  df_src2=df_transformation.filter(" _TYPE like '%Sequence%' ").select (col("_NAME").alias("src"))
  df_src=df_src1.unionAll(df_src2)
  df_TARGET=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "INSTANCE").load(sourcePath)
  df_tgt1=df_TARGET.select(col("_NAME").alias("tgt") , col("_TYPE").alias("Type") ).filter(" Type='TARGET'").select("tgt")

  vertices=df_transformation.select ( col("_NAME").alias("id")).unionAll( df_tgt1)
  edges=spark.sql("select `From Transformation` as src , `To Transformation` as dst  from df_con_group where `To Type` <> 'Source Qualifier' ")

  g = GraphFrame(vertices, edges)
  cnt=df_src1.count()
  vars = {i:v.src for i,v in enumerate(df_src.collect())}
  vars_tgt = {i:v.tgt for i,v in enumerate(df_tgt1.collect())}
  
  '''
  src1=vars[0]
  tgt1=vars_tgt[0]
  q1= "f1 = g.bfs(   fromExpr = \"id = '" + src1 + "' \",   toExpr = \"id = '" + tgt1 +"'\",   edgeFilter = \"src != 'joiner1'\",   maxPathLength = 10) "
  exec(q1)
  f1.createOrReplaceTempView("f1")
  q=''
  for item in f1.dtypes:  
    if item[0].startswith('v'):
      q +=  (item[0]) + ".id,"

  col_dtl1= q.rstrip(',')
  spark.sql(" select explode ( array ( {} )) from f1".format(col_dtl1)).show(10,False)
  df_branch1=spark.sql(" select explode ( array ( {} )) from f1".format(col_dtl1))

  if cnt > 1:
   src1=vars[1]
   tgt1=vars_tgt[1]
   q1= "f1 = g.bfs(   fromExpr = \"id = '" + src1 + "' \",   toExpr = \"id = '" + tgt1 +"'\",   edgeFilter = \"src != 'joiner1'\",   maxPathLength = 10) "
   exec(q1)
   f1.createOrReplaceTempView("f1")
   q=''
   for item in f1.dtypes:  
     if item[0].startswith('v'):
       q +=  (item[0]) + ".id,"

   col_dtl1= q.rstrip(',')
   spark.sql(" select explode ( array ( {} )) from f1".format(col_dtl1)).show()
   df_branch2=spark.sql(" select explode ( array ( {} )) from f1".format(col_dtl1))
  else:
   schema = StructType([     StructField("Object Name", StringType(), True), StructField("GroupNames", StringType(), False) ])
   df_branch2=spark.createDataFrame([], schema)


  df_branch1 = df_branch1.withColumn("key", F.monotonically_increasing_id())
  df_branch2 = df_branch2.withColumn("key", F.monotonically_increasing_id())
  d3 = df_branch1.union(df_branch2)
  '''
  i=0
  schema = StructType([     StructField("col", StringType(), True), StructField("key", IntegerType(), False) ])
  d3=spark.createDataFrame([], schema)

  schema1 = StructType([     StructField("col", StringType(), True) ])
  df_seq=spark.createDataFrame([], schema1)


  for x in range(df_src.count()):
   for y in range(df_tgt1.count()):
    print("Value of x " + str(x) + " value of y " + str(y) )
    src1=vars[x]
    tgt1=vars_tgt[y]
    print ("Running for source " + src1 + " and for target  "+ tgt1)
    q1= "f1 = g.bfs(   fromExpr = \"id = '" + src1 + "' \",   toExpr = \"id = '" + tgt1 +"'\",   edgeFilter = \"src != 'joiner1'\",   maxPathLength = 10) "
    exec(q1)
    f1.createOrReplaceTempView("f1")
    f1.printSchema()
    q=''
    for item in f1.dtypes:  
      if item[0].startswith('v'):
        print("Column Value : " + item[0])
        q +=  (item[0]) + ".id,"
    col_dtl1= q.rstrip(',')
    i += 1
    
    my_variables = {}
    print("Total length of Column Value " + str( len(col_dtl1)  ) ) 
    if len(col_dtl1) > 4 or f1.count()== 0:
     my_variables["w" + str(i)] = "df_branch"+str(i)+" = spark.sql(\" select explode ( array ( {} )) from f1\".format(col_dtl1))"
     spark.sql(" select explode ( array ( {} )) from f1".format(col_dtl1)).show()
     print(my_variables["w" + str(i)])
    	  
     exec(my_variables["w" + str(i)])
  
     add_id = {}
     add_id["w" + str(i)] = "df_branch"+str(i)+" =df_branch"+str(i)+".withColumn('key', row_number().over(Window.orderBy(monotonically_increasing_id())))"
     print(add_id["w" + str(i)])
     exec(add_id["w" + str(i)])
  
     show_id = {}
     show_id["w" + str(i)] = "df_branch"+str(i)+".show()"
     print(show_id["w" + str(i)])
     exec(show_id["w" + str(i)])

     schema_union = {}
     schema_union["w" + str(i)] = "d3 = d3.union(df_branch"+str(i)+") "
     print(schema_union["w" + str(i)])
     exec(schema_union["w" + str(i)])
    else:
     #if f1.count() > 0: 
     print("Found direct connection to target")    
     my_variables1 = {}
     my_variables1["w" + str(i)] = "df_target"+str(i)+" = spark.sql(\" select  explode(array(f1.from.id)) from f1\")"
     spark.sql(" select  explode(array(f1.from.id)) from f1").show()
     print(my_variables1["w" + str(i)])
     exec(my_variables1["w" + str(i)])

     seq_union = {}
     seq_union["w" + str(i)] = "df_seq = df_seq.union(df_target"+str(i)+") "
     print(seq_union["w" + str(i)])
     exec(seq_union["w" + str(i)])


    
  

  d3 = d3.orderBy('key').drop('key')
  w = Window().partitionBy("col").orderBy('col')

  d4 = d3.withColumn("key", F.monotonically_increasing_id())
  d4 = (d4
       .withColumn("dupe", F.row_number().over(w))
       .where("dupe == 1")
       .orderBy("key")
       .drop(*['key', 'dupe']))
  if df_seq.count() > 0:
   df_seq=df_seq.distinct()
   df_All=df_src1.unionAll(d4).unionAll(df_seq).unionAll(df_tgt1)
  else:
   df_All=df_src1.unionAll(d4).unionAll(df_tgt1)

  df_All.withColumn('id', row_number().over(Window.orderBy(monotonically_increasing_id()))).createOrReplaceTempView("df_All")


  df_trans1 = spark.sql("select distinct Transformation_Name , Transformation_Type from df_transformation")
  df_trans2=df_tgt1.withColumn("Transformation_Type" , lit('target'))

  df_trans = df_trans1.unionAll(df_trans2)
  df_trans.createOrReplaceTempView("df_trans")

  spark.sql("select id , A.src as Transformation_Name , case when Transformation_type ='Source Qualifier' then 'Source' else Transformation_type end as Type  from df_All A left outer join df_trans B on Transformation_Name=src order by id ").createOrReplaceTempView("df_transformation_order")
  df_map_transformation=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "MAPPING").load(sourcePath)
  df_map_transformation.select (col("_NAME").alias("Mapping_Name") ).createOrReplaceTempView("df_map1")

  spark.sql(" select  id , 'K1' as GroupKey , 'ADD_TRANSFORMATION' `Step Name` ,  Transformation_Name as `Object Name` , Type `Object Type` , '' as `Object Description` , Mapping_Name as `Step For`  from  df_transformation_order left outer join df_map1 B on 1=1 ").show()  
  

  #spark.sql("  select distinct 1 as `id` , 'K1' as `GroupKey` ,'CREATE_MAPPING' as `Step Name` ,Mapping_Name as `Object Name` ,'Mapping' as `Object Type` ,'This is Test Mapping' as `Object Description` , '' `Step For` from  df_map_transformation union all  select  2 as `id` , 'K1' as GroupKey , 'ADD_TRANSFORMATION' `Step Name` ,  NAME as `Object Name` , case when length(substr(Transformation_Type,1,instr(Transformation_Type,' ')-1)) < 2 then Transformation_Type else substr(Transformation_Type,1,instr(Transformation_Type,' ')-1) end `Object Type` , '' as `Object Description` ,Mapping_Name as `Step For`  from  df_map_transformation  where TRANSFORMATION_TYPE <> 'Source Definition'   union all select distinct 3 as `id` ,  'K1' as `GroupKey` ,'LINK_TRANSFORMATION' as `Step Name` ,'' as `Object Name` ,'' as `Object Type` ,'' as `Object Description` , Mapping_Name `Step For` from  df_map_transformation ").createOrReplaceTempView("df_map_transformation")
  spark.sql("  select distinct 0 as `id` , 'K1' as `GroupKey` ,'CREATE_MAPPING' as `Step Name` ,Mapping_Name as `Object Name` ,'Mapping' as `Object Type` ,'This is Test Mapping' as `Object Description` , '' `Step For` from  df_map_transformation union all  select  id , 'K1' as GroupKey , 'ADD_TRANSFORMATION' `Step Name` ,  Transformation_Name as `Object Name` , Type `Object Type` , '' as `Object Description` , Mapping_Name as `Step For`  from  df_transformation_order left outer join df_map1 B on 1=1   union all select distinct 1000 as `id` ,  'K1' as `GroupKey` ,'LINK_TRANSFORMATION' as `Step Name` ,'' as `Object Name` ,'' as `Object Type` ,'' as `Object Description` , Mapping_Name `Step For` from  df_map_transformation order by 1 ").createOrReplaceTempView("df_map_transformation") 
  
  df_transformation=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "TRANSFORMATION").load(sourcePath)
  df_transformation.select(col("_TYPE").alias("Transformation_Type")).createOrReplaceTempView("df_router")
  spark.sql(" select case when length(substr(Transformation_Type,1,instr(Transformation_Type,' ')-1)) < 2 then Transformation_Type else substr(Transformation_Type,1,instr(Transformation_Type,' ')-1) end as TYPE from df_router ").createOrReplaceTempView("df_trans_type")

  if spark.sql("select * from df_trans_type where TYPE= 'Router' ").count()==1:
   print("Router exists ")
   df_router=df_transformation.filter("_TYPE = 'Router'").select(col("_NAME").alias("Object Name"), col("GROUP._NAME").alias("GroupNames"))
   df_router.createOrReplaceTempView("df_router")
   df_router=spark.sql("select `Object Name` ,  concat_ws(',',GroupNames) as  GroupNames from df_router")
   df_router.show()
  else:
   schema = StructType([ StructField("Object Name", StringType(), True), StructField("GroupNames", StringType(), False) ])
   df_router=spark.createDataFrame([], schema)
   df_router.show()

 
  if spark.sql("select * from df_trans_type where TYPE= 'Union' ").count()==1:
   print("Union exists ")
   df_union_transformation=df_transformation.filter("_TYPE = 'Union'").select(col("_NAME").alias("Object Name"), col("GROUP._NAME").alias("GroupNames"))
   df_union_transformation.createOrReplaceTempView("df_union_transformation")
   df_union_transformation=spark.sql("select `Object Name` ,  concat_ws(',',GroupNames) as  GroupNames from df_union_transformation")
   df_union_transformation.show()
  else:
   print("Union doesnt exists ")
   schema = StructType([ StructField("Object Name", StringType(), True), StructField("GroupNames", StringType(), False) ])
   df_union_transformation=spark.createDataFrame([], schema)
   df_union_transformation.show()
 


  df_collect=df_router.union(df_union_transformation).createOrReplaceTempView("df_collect")

  spark.sql("select A.* ,B.GroupNames from df_map_transformation A left outer join  df_collect B on A.`Object Name` = B.`Object Name` order by id ").show()
  spark.sql("select A.GroupKey , A.`Step Name` , A.`Object Name` , A.`Object Type` , A.`Object Description` , A.`Step For` ,B.GroupNames from df_map_transformation A left outer join  df_collect B on A.`Object Name` = B.`Object Name` order by id ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  name="Main.csv"
  rename(output1,name)
 
  print ("Copying CSV files")

  directory = "Complete"
  parent_dir = output1 
  path = os.path.join(parent_dir, directory)
  os.mkdir(path)

  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #print ("Copying CSV files")
  #path=output1
  #cmd1="mkdir "+path+"Complete"
  #os.system(cmd1)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Generating common dataframes ............")
  
  df_transformation=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "TRANSFORMATION").load(sourcePath)
  df_transformation.select(col("_TYPE").alias("Transformation_Type") , col("_NAME").alias("Transformation_Name"),explode(arrays_zip(col("TRANSFORMFIELD._DATATYPE") ,col("TRANSFORMFIELD._NAME") ,col("TRANSFORMFIELD._EXPRESSION") , col("TRANSFORMFIELD._EXPRESSIONTYPE") ,col("TRANSFORMFIELD._SORTDIRECTION") , col("TRANSFORMFIELD._PRECISION") , col("TRANSFORMFIELD._SCALE") , col("TRANSFORMFIELD._PORTTYPE"), col("TRANSFORMFIELD._GROUP")  )) ).select("Transformation_Name" , "Transformation_Type" , col("col").cast("struct<DATATYPE:string,NAME:string,EXPRESSION:string,EXPRESSIONTYPE:string, SORTDIRECTION:string, PRECISION:bigint,SCALE:bigint,PORTTYPE:string , GROUP:string>" )).select("Transformation_Name" , "Transformation_Type" ,col("col.DATATYPE").alias("Column_DATATYPE") , col("col.NAME").alias("Column_NAME"), col("col.EXPRESSION").alias("Column_EXPRESSION") , col("col.EXPRESSIONTYPE").alias("Column_EXPRESSIONTYPE") ,  col("col.SORTDIRECTION").alias("Column_SORTDIRECTION")   ,col("col.PRECISION").alias("Column_PRECISION") , col("col.SCALE").alias("Column_SCALE") , col("col.PORTTYPE").alias("Column_PORTTYPE") , col("col.GROUP").alias("Column_GROUP")   ).createOrReplaceTempView("df_transformation")
  ##Added group condition for Joiner and Sorter details 
  spark.sql("select Transformation_Name , Transformation_Type , Column_DATATYPE , Column_NAME , Column_EXPRESSION , Column_EXPRESSIONTYPE ,Column_SORTDIRECTION, Column_PRECISION , Column_SCALE , Column_PORTTYPE , case when Transformation_Type = 'Joiner' and  instr(Column_PORTTYPE,'/MASTER') > 1 then 'Master'  when Transformation_Type = 'Joiner' and  instr(Column_PORTTYPE,'/MASTER') < 1 then 'Detail' else Column_GROUP end Column_GROUP from df_transformation").createOrReplaceTempView("df_transformation")
  df_map_transformation.select(col("_NAME").alias("Mapping Reference")).createOrReplaceTempView("df_mapping")
  spark.sql("select A.*,B.* from df_mapping A , df_transformation B where 1=1  ").createOrReplaceTempView("df_map_trans_port")

  df_transformation.select(col("_TYPE").alias("Transformation_Type") , col("_NAME").alias("Transformation_Name") , explode(arrays_zip(col("TABLEATTRIBUTE._NAME") ,col("TABLEATTRIBUTE._VALUE")   )) ).select("Transformation_Name" , "Transformation_Type" , col("col").cast("struct<TABLEATTRIBUTE_NAME:string,TABLEATTRIBUTE_VALUE:string>" )).select("Transformation_Name" , "Transformation_Type" , col("col.TABLEATTRIBUTE_NAME").alias("TABLEATTRIBUTE_NAME") , col("col.TABLEATTRIBUTE_VALUE").alias("TABLEATTRIBUTE_VALUE")  ).createOrReplaceTempView("df_attributes")
  spark.sql("select A.*,B.* from df_mapping A , df_attributes B where 1=1  ").createOrReplaceTempView("df_table_attributes")
  
  print("Generating Source side details .......")
  
  df_source=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "FOLDER").load(sourcePath)
  df_source.select(col("MAPPING._NAME").alias("Mapping Reference") ,  explode(arrays_zip( col("SOURCE._BUSINESSNAME") , col("SOURCE._NAME") , col("SOURCE._DATABASETYPE")  ))   ).select("Mapping Reference" , col("col").cast("struct<`Source Name`:string,`Source Type`:string,`Source Object`:string>" ) ).select ("Mapping Reference" , col("col.`Source Name`") , col("col.`Source Type`") , col("col.`Source Object`")  ).createOrReplaceTempView("df_source_primary")
  spark.sql("select * from df_table_attributes where TABLEATTRIBUTE_NAME = 'Source Filter' and  Transformation_Type = 'Source Qualifier' ").createOrReplaceTempView("df_filter")
  #spark.sql("select A.`Mapping Reference` , A.Transformation_Name as `Source Name` , 'Oracle_Robot' as `Connection Name` , 'Oracle' as `Source Type` , A.Transformation_Name as `Source Object` , TABLEATTRIBUTE_VALUE as `Query Filter` from df_filter A ").show()
  #spark.sql("select A.`Mapping Reference` , A.Transformation_Name as `Source Name` , 'Oracle_Robot' as `Connection Name` , 'Oracle' as `Source Type` , A.Transformation_Name as `Source Object` , TABLEATTRIBUTE_VALUE as `Query Filter` from df_filter A ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  
  spark.sql("select A.`Mapping Reference` , A.Transformation_Name as `Source Name` , Connection_Name as `Connection Name` , Connection_Subtype as `Source Type` , A.Transformation_Name as `Source Object` , '' as `SQL Query` , TABLEATTRIBUTE_VALUE as `Query Filter` from df_filter A left outer join df_wf_con B on A.Transformation_Name = B.Obejct_Name ").show()
  spark.sql("select A.`Mapping Reference` , A.Transformation_Name as `Source Name` , Connection_Name as `Connection Name` , Connection_Subtype as `Source Type` , A.Transformation_Name as `Source Object` , '' as `SQL Query` , TABLEATTRIBUTE_VALUE as `Query Filter` from df_filter A left outer join df_wf_con B on A.Transformation_Name = B.Obejct_Name ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Source.csv"
  rename(output1,name)
  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  
  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print("Generating target details ....")
  df_TARGET=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "INSTANCE").load(sourcePath)
  df_TARGET.select(col("_NAME").alias("Target Name") , col("_TRANSFORMATION_NAME").alias("Target Object") , col("_TYPE").alias("Type")).filter(" Type='TARGET'").createOrReplaceTempView("df_target1")
  df_mapping1=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "MAPPING").load(sourcePath)
  df_mapping1.select(col("_NAME").alias("Mapping Reference")).createOrReplaceTempView("df_mapping")

  #spark.sql("select `Mapping Reference` , `Target Name` ,'Oracle_Robot' as `Connection Name` ,'oracle' as `Target Type` , `Target Object` ,'' as CreateAtRuntime from df_target1 left outer join df_mapping where 1=1   ").show()
  #spark.sql("select `Mapping Reference` , `Target Name` ,'Oracle_Robot' as `Connection Name` ,'oracle' as `Target Type` , `Target Object` ,'' as CreateAtRuntime from df_target1 left outer join df_mapping where 1=1   ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  spark.sql("select `Mapping Reference` , `Target Name` ,  Connection_Name as `Connection Name` , Connection_Subtype as `Source Type` , `Target Object` ,'' as CreateAtRuntime from df_target1 A left outer join df_mapping on 1=1 left outer join df_wf_con B on A.`Target Name` = B.Obejct_Name  ").show()
  spark.sql("select `Mapping Reference` , `Target Name` ,  Connection_Name as `Connection Name` , Connection_Subtype as `Source Type` , `Target Object` ,'' as CreateAtRuntime from df_target1 A left outer join df_mapping on 1=1 left outer join df_wf_con B on A.`Target Name` = B.Obejct_Name  ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Target.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)
 
 
  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Getting Expression transformation details .....")
  spark.sql("select `Mapping Reference` , Transformation_Name as `Expression Name` , 'Output Field' as `Field Type` , Column_NAME as `Field Name` , Column_DATATYPE as Type ,Column_PRECISION as Precision , Column_SCALE as Scale , Column_EXPRESSION as `Expression Text` from df_map_trans_port where Transformation_Type = 'Expression' and Column_PORTTYPE = 'OUTPUT'  ").show()
  spark.sql("select `Mapping Reference` , Transformation_Name as `Expression Name` , 'Output Field' as `Field Type` , Column_NAME as `Field Name` , Column_DATATYPE as Type ,Column_PRECISION as Precision , Column_SCALE as Scale , Column_EXPRESSION as `Expression Text` from df_map_trans_port where Transformation_Type = 'Expression' and Column_PORTTYPE = 'OUTPUT'  ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Expression.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)


  spark.sql(" select `Mapping Reference` , `Transformation Name` , `File Rules - Operator` , `Field Selection Criteria` , concat_ws(',',A.Detail_Output) as Detail_Output from (  select  `Mapping Reference` , Transformation_Name as `Transformation Name` , 'Include' as `File Rules - Operator` ,'Named Fields' as `Field Selection Criteria` , collect_list(concat(Column_NAME)) as Detail_Output  from df_map_trans_port where Transformation_Type = 'Expression' and Column_PORTTYPE = 'INPUT/OUTPUT' group by `Mapping Reference` , Transformation_Name)A ").show()
  spark.sql(" select `Mapping Reference` , `Transformation Name` , `File Rules - Operator` , `Field Selection Criteria` , concat_ws(',',A.Detail_Output) as Detail_Output from (  select  `Mapping Reference` , Transformation_Name as `Transformation Name` , 'Include' as `File Rules - Operator` ,'Named Fields' as `Field Selection Criteria` , collect_list(concat(Column_NAME)) as Detail_Output  from df_map_trans_port where Transformation_Type = 'Expression' and Column_PORTTYPE = 'INPUT/OUTPUT' group by `Mapping Reference` , Transformation_Name)A ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Incoming Fields.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Get Filter details ...........")
  df_transformation.select(col("_TYPE").alias("Transformation_Type") , col("_NAME").alias("Transformation_Name") , explode(arrays_zip(col("TABLEATTRIBUTE._NAME") ,col("TABLEATTRIBUTE._VALUE")   )) ).select("Transformation_Name" , "Transformation_Type" , col("col").cast("struct<TABLEATTRIBUTE_NAME:string,TABLEATTRIBUTE_VALUE:string>" )).select("Transformation_Name" , "Transformation_Type" , col("col.TABLEATTRIBUTE_NAME").alias("TABLEATTRIBUTE_NAME") , col("col.TABLEATTRIBUTE_VALUE").alias("TABLEATTRIBUTE_VALUE")  ).createOrReplaceTempView("df_attributes")
  spark.sql("select A.*,B.* from df_mapping A , df_attributes B where 1=1  ").createOrReplaceTempView("df_table_attributes")
  spark.sql("select `Mapping Reference` , Transformation_Name as `Filter Name` , 'Advanced' as `Filter Type` , TABLEATTRIBUTE_VALUE as `Advanced Filter Condition` from df_table_attributes where `Transformation_Type` ='Filter' and TABLEATTRIBUTE_NAME= 'Filter Condition' ").show()

  spark.sql("select `Mapping Reference` , Transformation_Name as `Filter Name` , 'Advanced' as `Filter Type` , TABLEATTRIBUTE_VALUE as `Advanced Filter Condition` from df_table_attributes where `Transformation_Type` ='Filter' and TABLEATTRIBUTE_NAME= 'Filter Condition' ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  
  name="Filter.csv"
  rename(output1,name)

  print ("Copying CSV files")

  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Get Router details ...........")
  
  df_transformation.select(col("_TYPE").alias("Transformation_Type") , col("_NAME").alias("Transformation_Name") , explode(arrays_zip( col("GROUP._NAME") ,col("GROUP._TYPE") , col("GROUP._EXPRESSION")   ))    ).select ("Transformation_Name","Transformation_Type", col("col").cast("struct<GROUP_NAME:string,GROUP_TYPE:string,GROUP_EXPRESSION:string>" )   ).select("Transformation_Name","Transformation_Type" , col("col.GROUP_NAME").alias("GROUP_NAME") , col("col.GROUP_TYPE").alias("GROUP_TYPE") , col("col.GROUP_EXPRESSION").alias("GROUP_EXPRESSION")  ).createOrReplaceTempView("df_router")
  spark.sql("select A.*,B.* from df_mapping A , df_router B where 1=1  ").createOrReplaceTempView("df_router_map")
  spark.sql("select  `Mapping Reference` ,Transformation_Name  as `Router Name` , GROUP_NAME as `Group Name` , GROUP_EXPRESSION as `Group Condition`  from df_router_map where GROUP_EXPRESSION is not null ").show()
 
  spark.sql("select  `Mapping Reference` ,Transformation_Name  as `Router Name` , GROUP_NAME as `Group Name` , GROUP_EXPRESSION as `Group Condition`  from df_router_map where GROUP_EXPRESSION is not null ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  
  name="Router.csv"
  rename(output1,name)
  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)


  print ("Get Aggregator details ...........")
  spark.sql(" select distinct B.`Mapping_Name` as `Mapping Reference` ,  A.Transformation_Name as `Aggregator Name` , Column_NAME as `Port_Name` , Column_PRECISION as Precision , case when Column_EXPRESSIONTYPE = 'GROUPBY' then 'True' else 'False' end  Is_Aggregate_Column from df_transformation A  left outer join df_map1 B on 1=1  where A.Transformation_Type = 'Aggregator' " ).show()
  spark.sql(" select distinct B.`Mapping_Name` as `Mapping Reference` ,  A.Transformation_Name as `Aggregator Name` , Column_NAME as `Port_Name` , Column_PRECISION as Precision , case when Column_EXPRESSIONTYPE = 'GROUPBY' then 'True' else 'False' end  Is_Aggregate_Column from df_transformation A  left outer join df_map1 B on 1=1  where A.Transformation_Type = 'Aggregator' " ).coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)    

  name="Aggregator.csv"
  rename(output1,name)
  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  print ("Get Sorter details ...........")
  spark.sql(" select distinct B.`Mapping_Name` as `Mapping Reference`, Transformation_Name as `Sorter Name` , Column_NAME as `Sort Field` , Column_SORTDIRECTION as Order from df_transformation A left outer join df_map1 B on 1=1  where A.Transformation_Type = 'Sorter' order by 2 " ).show()
  spark.sql(" select distinct B.`Mapping_Name` as `Mapping Reference`, Transformation_Name as `Sorter Name` , Column_NAME as `Sort Field` , Column_SORTDIRECTION as Order from df_transformation A left outer join df_map1 B on 1=1  where A.Transformation_Type = 'Sorter' order by 2 " ).coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)    

  name="Sorter.csv"
  rename(output1,name)
  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  
  
  
  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Get Joiner details ...........")

  spark.sql("select `Mapping Reference` , Transformation_Name as `Joiner Name` ,  case when TABLEATTRIBUTE_NAME='Join Condition' then  substr(TABLEATTRIBUTE_VALUE,1,instr(TABLEATTRIBUTE_VALUE,'=')-1) end   as `Join Condition_Master` , case when TABLEATTRIBUTE_NAME='Join Condition' then  substr(TABLEATTRIBUTE_VALUE,instr(TABLEATTRIBUTE_VALUE,'=')+1) end as `Join Condition_Detail`  from df_table_attributes where Transformation_Type='Joiner' and TABLEATTRIBUTE_NAME in ( 'Join Condition' )   ").createOrReplaceTempView("df_joiner1")
  spark.sql("select `Mapping Reference` , Transformation_Name as `Joiner Name` , case when TABLEATTRIBUTE_NAME='Join Type' then  TABLEATTRIBUTE_VALUE end as `Join Type`  from df_table_attributes where Transformation_Type='Joiner' and TABLEATTRIBUTE_NAME in ( 'Join Type' )   ").createOrReplaceTempView("df_joiner2")
  spark.sql("select A.`Mapping Reference` , A.`Joiner Name` , B.`Join Type` , A.`Join Condition_Master` , A.`Join Condition_Detail` from df_joiner1 A left outer join df_joiner2 B on A.`Mapping Reference` = B.`Mapping Reference` and A.`Joiner Name` = B.`Joiner Name` ").show()

  spark.sql("select A.`Mapping Reference` , A.`Joiner Name` , B.`Join Type` , A.`Join Condition_Master` , A.`Join Condition_Detail` from df_joiner1 A left outer join df_joiner2 B on A.`Mapping Reference` = B.`Mapping Reference` and A.`Joiner Name` = B.`Joiner Name` ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Joiner.csv"
  rename(output1,name)

  print ("Copying CSV files")

  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Get Lineage details ...........")

  df_connector=spark.read.format('xml').option("rootTag", "POWERMART").option("rowTag", "CONNECTOR").load(sourcePath)
  df_connector.select( col("_FROMINSTANCE").alias("From Transformation") , col("_FROMINSTANCETYPE").alias("From Type") , col("_FROMFIELD").alias("From Field") , col("_TOINSTANCE").alias("To Transformation") , col("_TOINSTANCETYPE").alias("To Type") , col("_TOFIELD").alias("To field")  ).createOrReplaceTempView("df_connector")

  spark.sql(" select distinct A.`From Transformation` , A.`From Type` ,B.Column_GROUP as `From Group` , A.`To Transformation` , A.`To Type` , C.Column_GROUP as `To Group`   from df_connector A left outer join df_transformation B on A.`From Transformation` = B.Transformation_Name and A.`From Type`=B.Transformation_Type and A.`From Field` = B.Column_NAME  left outer join df_transformation C on A.`To Transformation` = C.Transformation_Name and A.`To Type`=C.Transformation_Type and A.`To Field` = C.Column_NAME     ").createOrReplaceTempView("df_con_group")
  spark.sql("select B.Mapping_Name as `Mapping Reference` , A.* from df_con_group A left outer join df_map1 B on 1=1 where `To Type` <> 'Source Qualifier' ").show()
  
  #spark.sql("select * from df_con_group where `To Type` <> 'Source Qualifier' ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  spark.sql(" select B.Mapping_Name as `Mapping Reference` , A.* from df_con_group A left outer join df_map1 B on 1=1 where `To Type` <> 'Source Qualifier' ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Transformation Lineage.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)


  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Get Target Field Mapping details ...........")
  spark.sql(" select distinct A.`From Transformation` , A.`From Type` ,B.Column_GROUP as `From Group`,A.`From Field` , A.`To Transformation` , A.`To Type` , C.Column_GROUP as `To Group` , A.`To Field`   from df_connector A left outer join df_transformation B on A.`From Transformation` = B.Transformation_Name and A.`From Type`=B.Transformation_Type and A.`From Field` = B.Column_NAME  left outer join df_transformation C on A.`To Transformation` = C.Transformation_Name and A.`To Type`=C.Transformation_Type and A.`To Field` = C.Column_NAME     ").createOrReplaceTempView("df_con_port_lineage")
  #spark.sql(" select A.`From Transformation` , A.`From Type` , A.`From Group` , A.`To Transformation`, A.`To Type` , A.`To Group` , concat_ws(',',A.`Field Map`) as `Field_Map` from (  select `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`          , collect_list(concat(`From Field`,'->',`To field`)) as `Field Map` from df_con_port_lineage  where `To Type` = 'Target Definition' group by `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`   )A ").show()
  #spark.sql(" select A.`From Transformation` , A.`From Type` , A.`From Group` , A.`To Transformation`, A.`To Type` , A.`To Group` , concat_ws(',',A.`Field Map`) as `Field_Map` from (  select `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`          , collect_list(concat(`From Field`,'->',`To field`)) as `Field Map` from df_con_port_lineage  where `To Type` = 'Target Definition' group by `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`   )A ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  #############Update for Router Group Field information ############################
  if spark.sql("select * from df_trans_type where TYPE= 'Router' ").count()==1:
    df_routre_field=df_transformation.filter("_TYPE = 'Router'").select(col("_TYPE").alias("Transformation_Type") , col("_NAME").alias("Transformation_Name") , explode(arrays_zip( col("TRANSFORMFIELD._NAME") ,col("TRANSFORMFIELD._REF_FIELD") , col("TRANSFORMFIELD._GROUP")   ))    ).select ("Transformation_Name","Transformation_Type", col("col").cast("struct<`From_Field`:string,`Actual_Field`:string,GROUP_NAME:string>" )   ).select("Transformation_Name","Transformation_Type" , col("col.From_Field").alias("Group_Field") , col("col.Actual_Field").alias("Router_Field") , col("col.GROUP_NAME").alias("GROUP_NAME")  )
    df_routre_fields=df_routre_field.filter("GROUP_NAME <> 'INPUT'")
    df_routre_fields.createOrReplaceTempView("df_routre_fields")
    spark.sql("select `From Transformation` ,`From Type` , `From Group` , case when `From Type` ='Router' and `From Group` is not null then Router_Field else `From Field` end `From Field`,`To Transformation` , `To Type` , `To Group`,`To Field`  from df_con_port_lineage A left outer join  df_routre_fields B on A.`From Transformation` = B.Transformation_Name and A.`From Type`=B.Transformation_Type and A.`From Group` = B.GROUP_NAME and A.`From Field` =B.Group_Field where `From Type`='Router' ").createOrReplaceTempView("df_con_port_lineage")


  #Updated Code
  spark.sql("select B.`Mapping Reference` ,  A.`Target Name` , A.`Field Map Option` , concat_ws(',',A.`Field Map`) as `Field_Map` from (    select `To Transformation` as `Target Name` , 'Manual' as `Field Map Option` , collect_list(concat(`From Field`,'->',`To field`)) as `Field Map` from df_con_port_lineage  where `To Type` = 'Target Definition'  group by `To Transformation` , 'Manual' ) A left outer join df_mapping B where 1=1  ").show()
  spark.sql("select B.`Mapping Reference` ,  A.`Target Name` , A.`Field Map Option` , concat_ws(',',A.`Field Map`) as `Field_Map` from (    select `To Transformation` as `Target Name` , 'Manual' as `Field Map Option` , collect_list(concat(`From Field`,'->',`To field`)) as `Field Map` from df_con_port_lineage  where `To Type` = 'Target Definition'  group by `To Transformation` , 'Manual' ) A left outer join df_mapping B where 1=1  ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  

  name="Target Field Mapping.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)


  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print ("Get port rename between transformation details ...........")

  spark.sql(" select distinct A.`From Transformation` , A.`From Type` ,B.Column_GROUP as `From Group`,A.`From Field` , A.`To Transformation` , A.`To Type` , C.Column_GROUP as `To Group` , A.`To Field`   from df_connector A left outer join df_transformation B on A.`From Transformation` = B.Transformation_Name and A.`From Type`=B.Transformation_Type and A.`From Field` = B.Column_NAME  left outer join df_transformation C on A.`To Transformation` = C.Transformation_Name and A.`To Type`=C.Transformation_Type and A.`To Field` = C.Column_NAME     ").createOrReplaceTempView("df_con_port_lineage")
  spark.sql(" select A.`From Transformation` , A.`From Type` , A.`From Group` , A.`To Transformation`, A.`To Type` , A.`To Group` , concat_ws(',',A.`Field Map`) as `Field_Map` from (  select `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`          , collect_list(concat(`From Field`,'->',`To field`)) as `Field Map` from df_con_port_lineage  where `From Field` <> `To field` and `To Type` <> 'Target Definition' group by `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`   )A ").show()
  spark.sql(" select A.`From Transformation` , A.`From Type` , A.`From Group` , A.`To Transformation`, A.`To Type` , A.`To Group` , concat_ws(',',A.`Field Map`) as `Field_Map` from (  select `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`          , collect_list(concat(`From Field`,'->',`To field`)) as `Field Map` from df_con_port_lineage  where `From Field` <> `To field` and `To Type` <> 'Target Definition' group by `From Transformation` ,`From Type`,`From Group` ,  `To Transformation` ,`To Type` ,`To Group`   )A ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Ports Rename Details.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)


  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)


  print("Generating excel output .....")
  path_complete=output1+"Complete/" 
  filename=path_complete+object_name+"_INFA_XML.xls"
  
  create_Excel(path_complete, filename)

  print ("Exiting the script .....")





