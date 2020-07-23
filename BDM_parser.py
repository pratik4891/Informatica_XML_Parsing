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
#import glob




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
  #print("Hello")
  sourcePath1 = sys.argv[1]
  sourcePath="file://" + sourcePath1
  output1 = sys.argv[2]
  output  = "file://" + output1
  output_transformation=output+"/"+"Transformation"
  output_write=output1+"/"+"Transformation"
  object_name = sys.argv[3]

  spark = SparkSession \
        .builder \
        .appName("PySpark Flatten XML Structure ") \
        .master("local[*]") \
        .config("spark.jars", "file:///usr/local/spark/spark/jars/spark-xml_2.12-0.9.0.jar") \
        .config("spark.executor.extraClassPath", "file:///usr/local/spark/spark/jars/spark-xml_2.12-0.9.0.jar") \
        .config("spark.executor.extraLibrary", "file:///usr/local/spark/spark/jars/spark-xml_2.12-0.9.0.jar") \
        .config("spark.driver.extraClassPath", "file:///usr/local/spark/spark/jars/spark-xml_2.12-0.9.0.jar") \
        .getOrCreate()

  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
  df1.withColumn("Transformation_Name",col("_name")).withColumn("Transformation_Type",col("_type")).select(col("Transformation_Name"),col( "Transformation_Type")).createOrReplaceTempView("df_transformation")

  df_map_transformation=spark.read.format('xml').option("rootTag", "imx:IMX").option("rowTag", "mapping:Mapping").option("charset", "UTF-8").load(sourcePath)

  df_map_transformation.withColumn("Mapping_Name", col("_name")).select(col("Mapping_Name") , explode(arrays_zip(col("instances.Instance._name"), col("instances.Instance._id") ))).select(col("Mapping_Name"), col("col").cast("struct<Transformation_Name:string,Transformation_Id:string>")).select(col("Mapping_Name"),col("col.Transformation_Name")).createOrReplaceTempView("df_map_transformation")
  print ("Check for router transformation  .......")
  router="routerInterfaces"
  if has_column(df1,router):
    print("Transformation Found ..")
    df_router=df1.withColumn("Transformation_Type",col("_type")).filter(" Transformation_Type = 'router:RouterTx'").select(col("_name").alias("Object Name") , col("routerInterfaces.RouterDataInterface._name").alias("GroupNames"))
    df_router.createOrReplaceTempView("df_router")
    df_router=spark.sql("select `Object Name` ,  concat_ws(',',GroupNames) as  GroupNames from df_router")
    df_router.show()
  else:
    schema = StructType([     StructField("Object Name", StringType(), True), StructField("GroupNames", StringType(), False) ])
    df_router=spark.createDataFrame([], schema)
  print ("Check for union transformation  .......")
  union="unionInterfaces" 

  if has_column(df1,union):
    print("Transformation Found ..")
    df_union_transformation=df1.withColumn("Transformation_Type",col("_type")).filter(" Transformation_Type = 'uniontx:UnionTx'").select(col("_name").alias("Object Name") , col("unionInterfaces.UnionDataInterface._name").alias("GroupNames"))
    df_union_transformation.createOrReplaceTempView("df_union_transformation")
    df_union_transformation=spark.sql("select `Object Name` ,  concat_ws(',',GroupNames) as  GroupNames from df_union_transformation")
    df_union_transformation.show()
  else:
    schema = StructType([     StructField("Object Name", StringType(), True), StructField("GroupNames", StringType(), False) ])
    df_union_transformation=spark.createDataFrame([], schema)

  df_collect=df_router.union(df_union_transformation).createOrReplaceTempView("df_collect")
  

  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
  df1.withColumn("Transformation_Name",col("_name")).withColumn("Transformation_Type",col("_type")).select(col("Transformation_Name"),col( "Transformation_Type")).createOrReplaceTempView("df_transformation")
  df_rel = spark.read.format('xml').option("rootTag", "instances").option("rowTag", "Instance").option("charset", "UTF-8").load(sourcePath)
  df2=df_rel.dtypes
  df3=spark.createDataFrame(df2, ['colname', 'datatype'])
  if df3.withColumn( "Ind" , when( col("datatype").like( "%OutlineLink:array%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="fromOutlineLinks").count()== 1:
   print("Array Type found ....")
   df_rel.withColumn ("Transformation_Name" , col("_name")).withColumn ("From" , col("_id")).withColumn ("To" , col("fromOutlineLinks.OutlineLink._toInstance")).select("Transformation_Name","From",explode_outer("To").alias("To")).createOrReplaceTempView("df_transformation1")
  else:
   print("Structure Type found ....")
   df_rel.withColumn ("Transformation_Name" , col("_name")).withColumn ("From" , col("_id")).withColumn ("To" , col("fromOutlineLinks.OutlineLink._toInstance")).select("Transformation_Name","From","To").createOrReplaceTempView("df_transformation1")

  df_map_transformation=spark.read.format('xml').option("rootTag", "imx:IMX").option("rowTag", "mapping:Mapping").load(sourcePath)
  df_map_transformation.withColumn("Mapping_Name", col("_name")).select(col("Mapping_Name") , explode(arrays_zip(col("instances.Instance._name"), col("instances.Instance._id") ))).select(col("Mapping_Name"), col("col").cast("struct<Transformation_Name:string,Transformation_Id:string>")).select(col("Mapping_Name"),col("col.Transformation_Name"),col("col.Transformation_Id")).createOrReplaceTempView("df_map_transformation")
  spark.sql("Select b.Mapping_Name as Mapping_Name ,a.* from df_transformation1 a , df_map_transformation b where a.Transformation_Name = b.Transformation_Name and a.From = b.Transformation_Id").createOrReplaceTempView("df_map_transformation_joined")
  df_transformation=spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").load(sourcePath)
  df_transformation.select(col("_name").alias("Transformation_Name") , col("_type").alias("Transformation_type")  ).createOrReplaceTempView("df_transformation_dtl")
  spark.sql(" select A.* , substr ( B.Transformation_type ,1, instr(B.Transformation_type , ':')-1) `To Type` from ( select Current_Mapping_Name as `Mapping Name`, Current_transformation `From Transformation` , substr ( B.Transformation_type ,1, instr(B.Transformation_type , ':')-1) `From Type`, '' `From Group` , A.Next_Transformation `To Transformation`   from (    select distinct a.Mapping_Name as Current_Mapping_Name , a.Transformation_Name  Current_transformation, b.Mapping_Name as Next_Mapping_Name , b.Transformation_Name Next_Transformation  from df_map_transformation_joined a , df_map_transformation_joined b where a.To = b.From )A , df_transformation_dtl B where A.Current_transformation=B.Transformation_Name ) A , df_transformation_dtl B where A.`To Transformation`=B.Transformation_Name ").show(100,False)

  vertices=df_rel.select ( col("_name").alias("id"))
  edges=spark.sql("  select  Current_transformation `src` ,  A.Next_Transformation `dst`   from (    select distinct a.Mapping_Name as Current_Mapping_Name , a.Transformation_Name  Current_transformation, b.Mapping_Name as Next_Mapping_Name , b.Transformation_Name Next_Transformation  from df_map_transformation_joined a , df_map_transformation_joined b where a.To = b.From )A , df_transformation_dtl B where A.Current_transformation=B.Transformation_Name  ")
  g = GraphFrame(vertices, edges)
  df_src1= spark.sql(" select `From Transformation` as src  from ( select Current_Mapping_Name as `Mapping Name`, Current_transformation `From Transformation` , substr ( B.Transformation_type ,1, instr(B.Transformation_type , ':')-1) `From Type`, '' `From Group` , A.Next_Transformation `To Transformation`   from (    select distinct a.Mapping_Name as Current_Mapping_Name , a.Transformation_Name  Current_transformation, b.Mapping_Name as Next_Mapping_Name , b.Transformation_Name Next_Transformation  from df_map_transformation_joined a , df_map_transformation_joined b where a.To = b.From )A , df_transformation_dtl B where A.Current_transformation=B.Transformation_Name ) A , df_transformation_dtl B where A.`To Transformation`=B.Transformation_Name and `From Type` = 'source'")
  df_tgt1= spark.sql("select Transformation_Name as tgt from df_transformation_dtl where substr ( Transformation_type ,1, instr(Transformation_type , ':')-1) ='target' ")
  cnt=df_src1.count()
  vars = {i:v.src for i,v in enumerate(df_src1.collect())}
  vars_tgt = {i:v.tgt for i,v in enumerate(df_tgt1.collect())}

  i=0
  schema = StructType([     StructField("col", StringType(), True), StructField("key", IntegerType(), False) ])
  d3=spark.createDataFrame([], schema)

  for x in range(df_src1.count()):
   for y in range(df_tgt1.count()):
    print("Value of x " + str(x) + " value of y " + str(y) )
    src1=vars[x]
    tgt1=vars_tgt[y]
    print ("Running for source " + src1 + " and for target  "+ tgt1)
    q1= "f1 = g.bfs(   fromExpr = \"id = '" + src1 + "' \",   toExpr = \"id = '" + tgt1 +"'\",   edgeFilter = \"src != 'joiner1'\",   maxPathLength = 10) "
    exec(q1)
    f1.createOrReplaceTempView("f1")
    q=''
    for item in f1.dtypes:  
      if item[0].startswith('v'):
        q +=  (item[0]) + ".id,"
    col_dtl1= q.rstrip(',')
    i += 1
    my_variables = {}
    my_variables["w" + str(i)] = "df_branch"+str(i)+" = spark.sql(\" select explode ( array ( {} )) from f1\".format(col_dtl1))"
    print(my_variables["w" + str(i)])
    spark.sql(" select explode ( array ( {} )) from f1".format(col_dtl1)).show()	  
    exec(my_variables["w" + str(i)])
  
    add_id = {}
    add_id["w" + str(i)] = "df_branch"+str(i)+" =df_branch"+str(i)+".withColumn('key', row_number().over(Window.orderBy(monotonically_increasing_id())))"
    print(add_id["w" + str(i)])
    exec(add_id["w" + str(i)])
  
    schema_union = {}
    schema_union["w" + str(i)] = "d3 = d3.union(df_branch"+str(i)+") "
    print(schema_union["w" + str(i)])
    exec(schema_union["w" + str(i)])

  d3 = d3.orderBy('key').drop('key')
  w = Window().partitionBy("col").orderBy('col')

  d4 = d3.withColumn("key", F.monotonically_increasing_id())
  d4 = (d4
       .withColumn("dupe", F.row_number().over(w))
       .where("dupe == 1")
       .orderBy("key")
       .drop(*['key', 'dupe']))

  df_All=df_src1.unionAll(d4).unionAll(df_tgt1)
  df_All.withColumn('id', row_number().over(Window.orderBy(monotonically_increasing_id()))).createOrReplaceTempView("df_All")

  spark.sql("select id , A.src as Transformation_Name , substr ( Transformation_type ,1, instr(Transformation_type , ':')-1) as Type  from df_All A left outer join df_transformation B on Transformation_Name=src order by id ").createOrReplaceTempView("df_transformation_order")
  spark.sql(" select distinct 0 as `Seq_No` , 'K1' as `GroupKey` , 'CREATE_MAPPING' as  `Step Name` , Mapping_Name as `Object Name`, 'Mapping' as `Object Type` , 'This is Test Mapping' as `Object Description` , ' ' as `Step For` , ' ' as `GroupNames` from df_map_transformation union all  select id as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as `Step Name`,a.Transformation_Name as `Object Name`, Type as `Object Type`, ' ' as `Object Description` , b.Mapping_Name as `Step For` , ' ' as `GroupNames` from df_transformation_order a , df_map_transformation b where a.Transformation_Name=b.Transformation_Name union all select distinct 1000 as `Seq_No` , 'K1' as `GroupKey` , 'LINK_TRANSFORMATION' as  `Step Name` , ' ' as `Object Name` , ' ' as `Object Type` , ' ' as `Object Description` , Mapping_Name as `Step For` , ' ' as `GroupNames` from df_map_transformation order by 1").show(20,False)

  spark.sql(" select distinct 0 as `Seq_No` , 'K1' as `GroupKey` , 'CREATE_MAPPING' as  `Step Name` , Mapping_Name as `Object Name`, 'Mapping' as `Object Type` , 'This is Test Mapping' as `Object Description` , '' as `Step For` , '' as `GroupNames` from df_map_transformation union all  select id as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as `Step Name`,a.Transformation_Name as `Object Name`, Type as `Object Type`, '' as `Object Description` , b.Mapping_Name as `Step For` , '' as `GroupNames` from df_transformation_order a , df_map_transformation b where a.Transformation_Name=b.Transformation_Name union all select distinct 1000 as `Seq_No` , 'K1' as `GroupKey` , 'LINK_TRANSFORMATION' as  `Step Name` , '' as `Object Name` , '' as `Object Type` , '' as `Object Description` , Mapping_Name as `Step For` , '' as `GroupNames` from df_map_transformation order by 1").createOrReplaceTempView("df_transf_dtls")

  
  
  spark.sql("select GroupKey, `Step Name` , A.`Object Name` , A.`Object Type` , A.`Object Description` , A.`Step For` , concat_ws(',',B.GroupNames)  as GroupNames from df_transf_dtls A left outer join df_collect B on A.`Object Name` = B.`Object Name` order by A.`Seq_No` ").show(13,False)


  spark.sql("select GroupKey, `Step Name` , A.`Object Name` , A.`Object Type` , A.`Object Description` , A.`Step For` , concat_ws(',',B.GroupNames)  as GroupNames from df_transf_dtls A left outer join df_collect B on A.`Object Name` = B.`Object Name` order by A.`Seq_No` ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  #spark.sql("select distinct 1 as `Seq_No` , 'K1' as `GroupKey` , 'CREATE_MAPPING' as  `Step Name` , Mapping_Name as `Object Name`, 'Mapping' as `Object Type` , 'This is Test Mapping' as `Object Description` , '' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 2 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Read_SALES' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_delta_update_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 3 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Read_SALES_TOTAL' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 4 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Sorter_SAMPLE_CUSTOMER' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 5 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Sorter_SAMPLE_CUSTOMER' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 6 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Join_Source_Target' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 7 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Expression' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 8 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Filter' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 9 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Router' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , 'NEW_INSERT,EXISTS_UPDATE' as `GroupNames` from df_map_transformation union all select distinct 10 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Target_Existing_Update' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 11 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Target_New_Insert' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 12 as `Seq_No` , 'K1' as `GroupKey` , 'ADD_TRANSFORMATION' as  `Step Name` , 'Target_Existing_INSERT' as `Object Name`, 'Source' as `Object Type` , '' as `Object Description` , 'm_SCD2_1' as `Step For` , '' as `GroupNames` from df_map_transformation union all select distinct 13 as `Seq_No` , 'K1' as `GroupKey` , 'LINK_TRANSFORMATION' as  `Step Name` , '' as `Object Name` , '' as `Object Type` , '' as `Object Description` , Mapping_Name as `Step For` , '' as `GroupNames` from df_map_transformation").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  


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

  #print ("Creating Directory......")
  #path=output1
  #cmd1="mkdir "+path+"Complete"
  #os.system(cmd1)  

  #print ("Copying CSV files")
  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print (" ======================================= Getting details for Source and target ============================================ ")
  
  ######Commenting previous way of source and target generation#########################

  print ("Showing Source details ....")
  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
  
  df2=df1.dtypes
  df3=spark.createDataFrame(df2, ['colname', 'datatype'])
  
  if df3.withColumn( "Ind" , when( col("datatype").like( "%_tableDefinitions:string%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="anonymousDso").count()== 1:
    if df3.withColumn( "Ind" , when( col("datatype").like( "%connectInfo%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="anonymousDso").count()== 1:
      df1.select( col("anonymousDso._name").alias("Table_Name")  ,col("anonymousDso._tableDefinitions").alias("Table_Id") ,   col("_type").alias("Type") , col("_name").alias("Transformation_Name") ,  col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectInfo").alias("ConnectionInfo") , col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectionName").alias("ConnectionName")  ).select(col("Transformation_Name"), "Type" , col("Table_Name") , col("Table_Id") , "ConnectionInfo","ConnectionName").createOrReplaceTempView("df_Src_tgt_details_tmp1")
    else:
      df1.select( col("anonymousDso._name").alias("Table_Name")  ,col("anonymousDso._tableDefinitions").alias("Table_Id") ,   col("_type").alias("Type") , col("_name").alias("Transformation_Name")).select(col("Transformation_Name"), "Type" , col("Table_Name") , col("Table_Id") , lit("").alias("ConnectionInfo"),lit("").alias("ConnectionName")).createOrReplaceTempView("df_Src_tgt_details_tmp1") 

  else:
    schema = StructType([ StructField("Transformation_Name", StringType(), True), StructField("Type", StringType(), False) , StructField("Table_Name", StringType(), False) , StructField("Table_Id", StringType(), False),StructField("ConnectionInfo", StringType(), False),StructField("ConnectionName", StringType(), False) ])
    spark.createDataFrame([], schema).createOrReplaceTempView("df_Src_tgt_details_tmp1")


  if df3.withColumn( "Ind" , when( col("datatype").like( "%_tableDefinition:string%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="anonymousDso").count()== 1:
    if df3.withColumn( "Ind" , when( col("datatype").like( "%connectInfo%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="anonymousDso").count()== 1:
      df1.select( col("anonymousDso._name").alias("Table_Name")  ,col("anonymousDso._tableDefinition").alias("Table_Id") ,   col("_type").alias("Type") , col("_name").alias("Transformation_Name") ,  col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectInfo").alias("ConnectionInfo") , col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectionName").alias("ConnectionName")  ).select(col("Transformation_Name"), "Type" , col("Table_Name") , col("Table_Id") , "ConnectionInfo","ConnectionName").createOrReplaceTempView("df_Src_tgt_details_tmp2")
    else:
      df1.select( col("anonymousDso._name").alias("Table_Name")  ,col("anonymousDso._tableDefinition").alias("Table_Id") ,   col("_type").alias("Type") , col("_name").alias("Transformation_Name") ).select(col("Transformation_Name"), "Type" , col("Table_Name") , col("Table_Id") , lit("").alias("ConnectionInfo"),lit("").alias("ConnectionName")).createOrReplaceTempView("df_Src_tgt_details_tmp2")        

  else:
    schema = StructType([ StructField("Transformation_Name", StringType(), True), StructField("Type", StringType(), False) , StructField("Table_Name", StringType(), False) , StructField("Table_Id", StringType(), False),StructField("ConnectionInfo", StringType(), False),StructField("ConnectionName", StringType(), False) ])
    spark.createDataFrame([], schema).createOrReplaceTempView("df_Src_tgt_details_tmp2")

  spark.sql("select * from df_Src_tgt_details_tmp1 union all select * from df_Src_tgt_details_tmp2 ").show()
  spark.sql("select * from df_Src_tgt_details_tmp1 union all select * from df_Src_tgt_details_tmp2 ").createOrReplaceTempView("df_Src_tgt_details")  

    
  schema = StructType([ StructField("Transformation_Name", StringType(), True), StructField("Type", StringType(), False) , StructField("Table_Name", StringType(), False) , StructField("Table_Id", StringType(), False),StructField("ConnectionInfo", StringType(), False),StructField("ConnectionName", StringType(), False) ])
  spark.createDataFrame([], schema).createOrReplaceTempView("df_Src_tgt_details1")
  df_mapping=spark.read.format('xml').option("rootTag", "imx:IMX").option("rowTag", "mapping:Mapping").option("charset", "UTF-8").load(sourcePath)
  df_mapping.select(col("_name").alias("Mapping Reference")).createOrReplaceTempView("df_mapping_name")

  print("Check for flat file information ......")
  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "datasource:FlatByteDataSource").option("charset", "UTF-8").load(sourcePath)
  defaultReaderConfig="defaultReaderConfig"
  if has_column(df1,defaultReaderConfig):
    df1.select(col("defaultGenerator._id").alias("dsoID") , col("defaultReaderConfig._sourceFileName").alias("SourceFileName") , col("defaultReaderConfig.`_sourceFileDirectory.`").alias("SourceFileDirectory") , col("defaultWriterConfig._targetFileName").alias("TargetFileName") , col("defaultWriterConfig.`_targetFileDirectory.`").alias("TargetFileDirectory")).createOrReplaceTempView("df_flat_source") 
  else:
    schema = StructType([ StructField("dsoID", StringType(), True), StructField("SourceFileName", StringType(), False) , StructField("SourceFileDirectory", StringType(), False) , StructField("TargetFileName", StringType(), False),StructField("TargetFileDirectory", StringType(), False) ])
    spark.createDataFrame([], schema).createOrReplaceTempView("df_flat_source")
  
  print("Showing Flat file realted details only ......")
  spark.sql("select * from df_flat_source").show() 
  
  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").load(sourcePath)
  df1.select(col("_name").alias("sourcename"),col("_type").alias("Type") ,col("_dso").alias("dsoId")).show()
  df1.select(col("_name").alias("sourcename"),col("_type").alias("Type") ,col("_dso").alias("dsoId")).createOrReplaceTempView("df_flat_dsoID")
  
  if spark.sql("select * from df_flat_source").count() > 0 :
    print("Flat file source found .....")
    spark.sql("select sourcename as `Source Name` , '' as `Connection Name` , 'FlatFile' as `Source Type` , SourceFileName as `Source Object` , SourceFileDirectory as `SQL Query` , '' as `Query Filter` , Type from df_flat_dsoID A , df_flat_source B where A.dsoId = B.dsoID").show()
    spark.sql("select sourcename as `Source Name` , '' as `Connection Name` , 'FlatFile' as `Source Type` , SourceFileName as `Source Object` , SourceFileDirectory as `SQL Query` , '' as `Query Filter` , Type from df_flat_dsoID A , df_flat_source B where A.dsoId = B.dsoID").createOrReplaceTempView("df_flat_source_out")
  else:
    print("No Flat file source found .....")  
    schema = StructType([ StructField("Source Name", StringType(), True), StructField("Connection Name", StringType(), False) , StructField("Source Type", StringType(), False),StructField("Source Object", StringType(), False) , StructField("SQL Query", StringType(), False) , StructField("Query Filter", StringType(), False),StructField("Type", StringType(), False) ])
    spark.createDataFrame([], schema).createOrReplaceTempView("df_flat_source_out")
  
  print("Showing Complete flat file realted details only ......")
  spark.sql("select `Source Name` , `Connection Name` , `Source Type` ,`Source Object` ,`SQL Query` ,`Query Filter` , Type from df_flat_source_out union select Transformation_Name as `Source Name` , ConnectionName as `Connection Name` , substr(ConnectionInfo,instr(ConnectionInfo,':')+1) as `Source Type` , Table_Name as `Source Object` ,'' as `SQL Query` , '' as `Query Filter` , Type from df_Src_tgt_details").createOrReplaceTempView("df_Src_tgt_details")
  anonymousDso="_dso"
  if has_column(df1,anonymousDso):
    print("Relational details found .........")
    if spark.sql("select * from df_Src_tgt_details").count() > 0:
      print ("Found entries in source target dataframe .....")
      #spark.sql("select `Mapping Reference` ,Transformation_Name as `Source Name` , ConnectionName as `Connection Name` , substr(ConnectionInfo,instr(ConnectionInfo,':')+1) as `Source Type` , Table_Name as `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%SourceTx%' ").show()
      #spark.sql("select `Mapping Reference` ,Transformation_Name as `Source Name` , ConnectionName as `Connection Name` , substr(ConnectionInfo,instr(ConnectionInfo,':')+1) as `Source Type` , Table_Name as `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%SourceTx%' ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
      spark.sql("select `Mapping Reference` ,`Source Name` , `Connection Name` , `Source Type` , `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%SourceTx%' and `Source Type` is not null ").show()
      spark.sql("select `Mapping Reference` ,`Source Name` , `Connection Name` , `Source Type` , `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%SourceTx%' and `Source Type` is not null ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  
      name="Source.csv"
      rename(output1,name)
  
      print ("Copying CSV files")
      src = output1 + name
      dst = output1 + directory +"/"+ name	
      shutil.move(src, dst)

      #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
      #os.system(cmd4)


      print ("Showing Target details ....")
      #spark.sql("select `Mapping Reference` , Transformation_Name as `Source Name` , ConnectionName as `Connection Name` , substr(ConnectionInfo,instr(ConnectionInfo,':')+1) as `Source Type` , Table_Name as `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%TargetTx%' ").show()
      #spark.sql("select `Mapping Reference` , Transformation_Name as `Source Name` , ConnectionName as `Connection Name` , substr(ConnectionInfo,instr(ConnectionInfo,':')+1) as `Source Type` , Table_Name as `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%TargetTx%' ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
      #spark.sql("select `Mapping Reference` ,`Source Name` , `Connection Name` , `Source Type` , `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%TargetTx%' ").show()
      #spark.sql("select `Mapping Reference` ,`Source Name` , `Connection Name` , `Source Type` , `Source Object` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%TargetTx%' ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
      spark.sql("select `Mapping Reference` ,`Source Name` , `Connection Name` , `Source Type` , `Source Object` ,case when `Source Type`='FlatFile' then `SQL Query` else null end as `Directory` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%TargetTx%' and `Source Type` is not null ").show(10,False)
      spark.sql("select `Mapping Reference` ,`Source Name` , `Connection Name` , `Source Type` , `Source Object` ,case when `Source Type`='FlatFile' then `SQL Query` else null end as `Directory` from df_Src_tgt_details left outer join df_mapping_name on 1=1 where Type like '%TargetTx%' and `Source Type` is not null ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
      name="Target.csv"
      rename(output1,name)
  
      print ("Copying CSV files")
      src = output1 + name
      dst = output1 + directory +"/"+ name	
      shutil.move(src, dst)


      #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
      #os.system(cmd4)  

  print ("Starting transformation level details .................")

  print("Getting Filter Transformation Details .....")

  filter="filterInterface"
  if has_column(df1,filter):

    print ("Printing Filter Transformation Details ...")
    df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
    df1.withColumn("Transformation_Name",col("_name")).withColumn("Transformation_Type",col("_type")).withColumn("Filter_Condition",col("_filterCondition")).filter("Transformation_Type = 'filter:FilterTx'").select("Transformation_Name","Transformation_Type", "Filter_Condition").createOrReplaceTempView("df_filter")
    spark.sql(" select B.*,A.Transformation_Name ,'Advanced' as `Filter Type` ,  A.Filter_Condition as `Advanced Filter Condition`  from df_filter A ,  df_mapping_name B where 1=1 ").show()

    spark.sql(" select B.*,A.Transformation_Name as `Filter Name`,'Advanced' as `Filter Type` ,  A.Filter_Condition as `Advanced Filter Condition`  from df_filter A ,  df_mapping_name B where 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

    name="Filter.csv"
    rename(output1,name)
    
    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)


    #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
    #os.system(cmd4)
  
  print("Get SQL transformation .....")
  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
  sqltrans="sqlInterfaces"
  if has_column(df1,sqltrans):
    df1.select(col("_active").alias("Is Active") , lit("").alias("Auto Commit") , col("_isContinueOnErrorSelected").alias("Is Continue on Error") , col("_maxOutRowCount").alias("Max Output Row Count") , lit("").alias("Transformation Scope") , lit("").alias("Add NumRowsAffected Port") , col("_type").alias("Transformation_Type") , col("_sqlQuery").alias("SQL Type") , col("_name").alias("Transformation Name")).filter("Transformation_Type = 'sqltx:SqlTx'").select("Transformation Name" , lit("").alias("Connection Name") , "SQL Type" , lit("").alias("Field Mapping") , "Is Active" , "Auto Commit","Is Continue on Error" , "Max Output Row Count","Transformation Scope","Add NumRowsAffected Port" ).show()
    df1.select(col("_active").alias("Is Active") , lit("").alias("Auto Commit") , col("_isContinueOnErrorSelected").alias("Is Continue on Error") , col("_maxOutRowCount").alias("Max Output Row Count") , lit("").alias("Transformation Scope") , lit("").alias("Add NumRowsAffected Port") , col("_type").alias("Transformation_Type") , col("_sqlQuery").alias("SQL Type") , col("_name").alias("Transformation Name")).filter("Transformation_Type = 'sqltx:SqlTx'").select("Transformation Name" , lit("").alias("Connection Name") , "SQL Type" , lit("").alias("Field Mapping") , "Is Active" , "Auto Commit","Is Continue on Error" , "Max Output Row Count","Transformation Scope","Add NumRowsAffected Port" ).createOrReplaceTempView("df_sql")
    
    spark.sql(" select B.*,A.*  from df_sql A ,  df_mapping_name B where 1=1 ").show()
    spark.sql(" select B.*,A.*  from df_sql A ,  df_mapping_name B where 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
    
    df1.select ( col("_name").alias("Transformation_Name"),explode (col("sqlInterfaces.SqlDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,sqlFields:struct<SqlField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_sqlPortNativeType:string,_type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") , explode (col("col.sqlFields.SqlField"))).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,sqlPortNativeType:string,type1:string,upper1:string>") ).select(col("Transformation_Name").alias("Transformation Name") , col("col.name1").alias("Field Name") , col("Group_Name") , col("col.type1").alias("Type") , col("col.precision1").alias("Precision")  ).show()

    
    
    name="SQL.csv"
    rename(output1,name)
    
    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)

  sqlt="lookupInterface" 
  if has_column(df1,sqlt):
    print ("Printing Lookup Transformation Details ...")
    
    df1 =spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)    
    multipleMatchHandling="multipleMatchHandling"
    if has_column(df1,multipleMatchHandling):
      #df1.select( col("_name").alias("Lookup Name"), col("anonymousDso._id").alias("dsoid") , col("anonymousDso._name").alias("dsoname") ,col("_multipleMatchHandling").alias("Multiple Matches"), col("_type").alias("Type") , col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectionName").alias("Connection Name") ,  col("_lookupCondition").alias("Lookup Condition") ).filter("Type = 'lookup:LookupTx'").createOrReplaceTempView("df_lkp_basic")
      df1.select( col("_name").alias("Lookup Name"), col("anonymousDso._name").alias("dsoname") ,col("_multipleMatchHandling").alias("Multiple Matches"), col("_type").alias("Type") , col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectionName").alias("Connection Name") ,  col("_lookupCondition").alias("Lookup Condition") ).filter("Type = 'lookup:LookupTx'").createOrReplaceTempView("df_lkp_basic")
    else:
      #df1.select( col("_name").alias("Lookup Name"), col("anonymousDso._id").alias("dsoid") , col("anonymousDso._name").alias("dsoname") ,col("_type").alias("Type") ,lit("").alias("Multiple Matches"), col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectionName").alias("Connection Name") ,  col("_lookupCondition").alias("Lookup Condition") ).filter("Type = 'lookup:LookupTx'").createOrReplaceTempView("df_lkp_basic")
      df1.select( col("_name").alias("Lookup Name"), col("anonymousDso._name").alias("dsoname") ,col("_type").alias("Type") ,lit("").alias("Multiple Matches"), col("anonymousDso.defaultConfigs.DSORuntimeConfig._connectionName").alias("Connection Name") ,  col("_lookupCondition").alias("Lookup Condition") ).filter("Type = 'lookup:LookupTx'").createOrReplaceTempView("df_lkp_basic")
    returnPort="returnPort"
    if has_column(df1,returnPort):
      df_lkp_return=df1.select( col("_name").alias("Lookup Name"), col("_returnPort").alias("Return Fields") ).filter("Type = 'lookup:LookupTx'")
    else:
      schema = StructType([StructField("Lookup Name" , StringType(), True), StructField("Return Fields", StringType(), False)])
      df_lkp_return=spark.createDataFrame([], schema)

    df_lkp_return.createOrReplaceTempView("df_lkp_return")

    dynamiclkp="updateDynamicCacheCondition"
    if has_column(df1,dynamiclkp):
      df_lkp_dynamic=df1.select( col("_name").alias("Lookup Name"), col("_updateDynamicCacheCondition").alias("Is Dynamic Lookup") ).filter("Type = 'lookup:LookupTx'")
    else:
      schema = StructType([StructField("Lookup Name" , StringType(), True), StructField("Is Dynamic Lookup", StringType(), False)])
      df_lkp_dynamic=spark.createDataFrame([], schema)
    
    df_lkp_dynamic.createOrReplaceTempView("df_lkp_dynamic")
     
    df2=df1.dtypes
    df3=spark.createDataFrame(df2, ['colname', 'datatype'])
    if df3.withColumn( "Ind" , when( col("datatype").like( "%filterCondition%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="anonymousDso").count()== 1:
      df_sql_filter=df1.select( col("_name").alias("Lookup Name") ,  col("anonymousDso._filterCondition").alias("Source Filter"))
    else:
      schema = StructType([     StructField("Lookup Name" , StringType(), True), StructField("Source Filter", StringType(), False)])
      df_sql_filter=spark.createDataFrame([], schema)

    df_sql_filter.createOrReplaceTempView("df_sql_filter")
    
    df2=df1.dtypes
    df3=spark.createDataFrame(df2, ['colname', 'datatype'])
    if df3.withColumn( "Ind" , when( col("datatype").like( "%_sqlQuery%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="anonymousDso").count()== 1:
      df_sql_override=df1.select( col("_name").alias("Lookup Name") ,  col("anonymousDso._sqlQuery").alias("SQL Override"))
    else:
      schema = StructType([     StructField("Lookup Name" , StringType(), True), StructField("SQL Override", StringType(), False)])
      df_sql_override=spark.createDataFrame([], schema)

    df_sql_override.createOrReplaceTempView("df_sql_override")

  javatrans="javaInterfaces"
  if has_column(df1,javatrans):
   
    df1.select(col("_type").alias("Transformation_Type") , col("_name").alias("Transformation Name"),col("_active").alias("Is Active") , col("_scope").alias("Transformation Scope") , lit("").alias("Enable High Precision") , lit("").alias("ClassPath") , col("_importPackagesSnippet").alias("Import Packages") , col("_helperCodeSnippet").alias("Helper Code") , col("_onInputRowMethodSnippet").alias("On Input Row") , col("_onEndOfDataMethodSnippet").alias("At the End of Data") , lit("").alias("On Receiving Transaction")).filter("Transformation_Type = 'javatx:JavaTx'").select("Transformation Name" , "Is Active" , "Transformation Scope" , "Enable High Precision" ,"ClassPath" , "Import Packages","Helper Code" , "On Input Row", "At the End of Data", "On Receiving Transaction" ).show()
    df1.select(col("_type").alias("Transformation_Type") , col("_name").alias("Transformation Name"),col("_active").alias("Is Active") , col("_scope").alias("Transformation Scope") , lit("").alias("Enable High Precision") , lit("").alias("ClassPath") , col("_importPackagesSnippet").alias("Import Packages") , col("_helperCodeSnippet").alias("Helper Code") , col("_onInputRowMethodSnippet").alias("On Input Row") , col("_onEndOfDataMethodSnippet").alias("At the End of Data") , lit("").alias("On Receiving Transaction")).filter("Transformation_Type = 'javatx:JavaTx'").select("Transformation Name" , "Is Active" , "Transformation Scope" , "Enable High Precision" ,"ClassPath" , "Import Packages","Helper Code" , "On Input Row", "At the End of Data", "On Receiving Transaction" ).createOrReplaceTempView("df_java_tab")
    spark.sql(" select B.*, A.*  from df_java_tab A ,  df_mapping_name B where 1=1 ").show()
    spark.sql(" select B.*, A.*  from df_java_tab A ,  df_mapping_name B where 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

    name="Java.csv"
    rename(output1,name)
    
    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)
     
    


  sqlt="lookupInterface" 
  if has_column(df1,sqlt):
 
    print("Showing Lookup transformation details ..........")
    spark.sql("select * from df_lkp_basic").show()
    spark.sql("select * from df_lkp_return").show()
    spark.sql("select * from df_lkp_dynamic").show()
    spark.sql("select * from df_sql_filter").show()
    spark.sql("select * from df_sql_override").show()

    spark.sql("select A.`Lookup Name` , 'No' as `Is Unconnected Lookup` , `Connection Name` , dsoname as `Lookup Object` , `Multiple Matches` , `SQL Override` , `Source Filter` , 'No' as `Caching Enabled` , case when `Is Dynamic Lookup`='true' then 'No' else 'Yes' end `Is Lookup Static` , `Is Dynamic Lookup` , `Return Fields` from df_lkp_basic A left outer join df_lkp_return B on A.`Lookup Name`=B.`Lookup Name` left outer join df_lkp_dynamic C on A.`Lookup Name`=C.`Lookup Name` left outer join df_sql_filter D on A.`Lookup Name`=D.`Lookup Name` left outer join df_sql_override E on A.`Lookup Name`=E.`Lookup Name` ").show()

    spark.sql("select A.`Lookup Name` , 'No' as `Is Unconnected Lookup` , `Connection Name` , dsoname as `Lookup Object` , `Multiple Matches` , `SQL Override` , `Source Filter` , 'No' as `Caching Enabled` , case when `Is Dynamic Lookup`='true' then 'No' else 'Yes' end `Is Lookup Static` , `Is Dynamic Lookup` , `Return Fields` from df_lkp_basic A left outer join df_lkp_return B on A.`Lookup Name`=B.`Lookup Name` left outer join df_lkp_dynamic C on A.`Lookup Name`=C.`Lookup Name` left outer join df_sql_filter D on A.`Lookup Name`=D.`Lookup Name` left outer join df_sql_override E on A.`Lookup Name`=E.`Lookup Name` ").createOrReplaceTempView("df_lkp_cmplt")
    spark.sql(" select B.*, A.*  from df_lkp_cmplt A ,  df_mapping_name B where 1=1 ").show()
    spark.sql(" select B.*, A.*  from df_lkp_cmplt A ,  df_mapping_name B where 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

    name="Lookup.csv"
    rename(output1,name)
    
    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)

    #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
    #os.system(cmd4)
 
	
  router="routerInterfaces" 
  if has_column(df1,router):
    print ("Printing Router Transformation Details ...")
    df1 =spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)    
    #df1.withColumn("Transformation_Name",col("_name")).withColumn("Transformation_Type",col("_type")).withColumn("Group Name",col("routerInterfaces.RouterDataInterface._name")).withColumn("Group Condition",col("routerInterfaces.RouterDataInterface._dataCondition")).filter("Transformation_Type = 'router:RouterTx'").select("Transformation_Name","Transformation_Type",  explode(arrays_zip("Group Name" ,"Group Condition")) ).select ("Transformation_Name","Transformation_Type" , col("col").cast("struct<GroupName:string , GroupCondition:string>") ).select ("Transformation_Name","Transformation_Type" , col("col.GroupName").alias("Group Name") , col("col.GroupCondition").alias("Group Condition")).where(col("Group Condition").isNotNull()).coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
    df1.withColumn("Transformation_Name",col("_name")).withColumn("Transformation_Type",col("_type")).withColumn("Group Name",col("routerInterfaces.RouterDataInterface._name")).withColumn("Group Condition",col("routerInterfaces.RouterDataInterface._dataCondition")).filter("Transformation_Type = 'router:RouterTx'").select("Transformation_Name","Transformation_Type",  explode(arrays_zip("Group Name" ,"Group Condition")) ).select ("Transformation_Name","Transformation_Type" , col("col").cast("struct<GroupName:string , GroupCondition:string>") ).select ("Transformation_Name","Transformation_Type" , col("col.GroupName").alias("Group Name") , col("col.GroupCondition").alias("Group Condition")).where(col("Group Condition").isNotNull()).createOrReplaceTempView("df_routrer_output")
    spark.sql("select B.*, A.Transformation_Name as `Router Name` , `Group Name` , `Group Condition` from df_routrer_output A ,  df_mapping_name B where 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

    name="Router.csv"
    rename(output1,name)
    
    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)

    #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
    #os.system(cmd4)
	
  joiner="joinerInterfaces"
  if has_column(df1,joiner):
    print ("Printing Joiner Transformation Details ...")
    df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)    
    jointype="joinType"
    if has_column(df1,jointype):
      df1.withColumn("Transformation_Name",col("_name")).withColumn("Transformation_Type",col("_type")).withColumn("Join_Type",col("_joinType")).withColumn("Join_Condition",col("_joinCondition")).withColumn("Is_Join_Case_Sensitive",col("_caseSensitive")).filter("Transformation_Type = 'joiner:JoinerTx'").select("Transformation_Name","Transformation_Type", "Join_Condition" , "Is_Join_Case_Sensitive" , "Join_Type").createOrReplaceTempView("df_joiner")
    else:
      df1.withColumn("Transformation_Name",col("_name")).withColumn("Transformation_Type",col("_type")).withColumn("Join_Type",lit("")).withColumn("Join_Condition",col("_joinCondition")).withColumn("Is_Join_Case_Sensitive",col("_caseSensitive")).filter("Transformation_Type = 'joiner:JoinerTx'").select("Transformation_Name","Transformation_Type", "Join_Condition" , "Is_Join_Case_Sensitive" , "Join_Type").createOrReplaceTempView("df_joiner")
    
    spark.sql(" select B.*,A.Transformation_Name as `Joiner Name` ,Join_Type as `Join Type` ,  A.Join_Condition as `Join Condition_Master` , substr(A.Join_Condition, 1 , instr(A.Join_Condition,'+')-1) as `Join Condition_Details` ,  substr(A.Join_Condition, instr(A.Join_Condition,'+')+5) as `Join Condition_Master` from df_joiner A ,  df_mapping_name B where 1=1 ").show()

    spark.sql(" select B.*,A.Transformation_Name as `Joiner Name` ,Join_Type as `Join Type`  , substr(A.Join_Condition, instr(A.Join_Condition,'+')+5) as `Join Condition_Master` ,  substr(A.Join_Condition, 1 , instr(A.Join_Condition,'+')-1) as `Join Condition_Details` from df_joiner A ,  df_mapping_name B where 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

    name="Joiner.csv"
    rename(output1,name)

    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)


    #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
    #os.system(cmd4)



  aggregate="aggregatorInterface"
  if has_column(df1,aggregate):
    print ("Printing Aggregator Transformation Details ...")
    df2=df1.dtypes
    df3=spark.createDataFrame(df2, ['colname', 'datatype'])
    if df3.withColumn( "Ind" , when( col("datatype").like( "%TransformationFieldRef:array%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="groupByFields").count()== 1:
      print("Multiple aggregated columns found ")
      df2=df1.select( col("_name") , col("groupByFields.fields.TransformationFieldRef._transformationField").alias("Id1"), explode( arrays_zip( col("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._name"), col("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._precision") , col ("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._input") ,  col ("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._id") ))).select(col("_name"),col("Id1"), col("col").cast("struct<Port_Name:string,Precision:bigint,Is_input_Port:string,Id:string>")).select(col("_name"),col("Id1"),col("col.Port_Name"),col("col.Precision") , col("col.Is_input_Port"),col("col.Id")).withColumn("Is_Aggregate_Column", when( array_contains(col("Id1"), col("Id")) ,  "True" ).otherwise("False")).drop(col("Id1"))

    else:
      print("Single aggregated column found ")
      df2=df1.select(col("_name") ,col("groupByFields.fields.TransformationFieldRef._transformationField").alias("Aggregate_Column"), explode(arrays_zip(col("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._name"), col("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._precision") , col ("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._input") ,  col ("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._id") ))).select(col("_name"),col("Aggregate_Column"), col("col").cast("struct<Port_Name:string,Precision:bigint,Is_input_Port:string,Id:string>")).select(col("_name"), col("Aggregate_Column"),  col("col.Port_Name"),col("col.Precision") , col("col.Is_input_Port"),col("col.Id") ).withColumn("Is_Aggregate_Column", when(col("Aggregate_Column") == col("Id"),"True").otherwise("False"))
    df2.createOrReplaceTempView("df_aggregate")
    df2.show()
    spark.sql(" select B.*,A._name as `Aggregator Name` ,Port_Name as `Port_Name`  , Precision as `Precision` ,  Is_Aggregate_Column as `Is_Aggregate_Column` from df_aggregate A ,  df_mapping_name B where 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
    name="Aggregator.csv"
    rename(output1,name)
     
    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)

 
    #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
    #os.system(cmd4)

  ###### Added Sorter details 

  sorter="sorterDataInterface"
  if has_column(df1,sorter):
    df_sorter = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
    ##df_sorter.select(col("_name").alias("Transformation_Name") , col("sortKeys.fieldRefs.SortKeyFieldRef.fieldRef._transformationField").alias("Column_Id")).filter("Column_Id <> ''").show()
    ##df_sorter.select(col("_name").alias("Transformation_Name") , col("sortKeys.fieldRefs.SortKeyFieldRef.fieldRef._transformationField").alias("Column_Id")).filter("Column_Id <> ''").createOrReplaceTempView("df_sorter_ascn")

    df2=df_sorter.dtypes
    df3=spark.createDataFrame(df2, ['colname', 'datatype'])

    if df3.withColumn( "Ind" , when( col("datatype").like( "%array%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="sortKeys").count()== 1: 
      print("Multiple Sort keys found .......")
      df_sorter.select(col("_name").alias("Transformation_Name") , col("sortKeys.fieldRefs.SortKeyFieldRef.fieldRef._transformationField").alias("Column_Id")).select("Transformation_Name" , explode("Column_Id").alias("Column_Id")).createOrReplaceTempView("df_sorter_ascn")
    else : 
      print("Single sort key found ......")
      df_sorter.select(col("_name").alias("Transformation_Name") , col("sortKeys.fieldRefs.SortKeyFieldRef.fieldRef._transformationField").alias("Column_Id")).select("Transformation_Name" , col("Column_Id").alias("Column_Id")).createOrReplaceTempView("df_sorter_ascn")

    df_sorter.select(col("_name").alias("Transformation_Name") , explode(arrays_zip(col("sorterDataInterface.SorterDataInterface.sorterFields.SorterField._name"), col("sorterDataInterface.SorterDataInterface.sorterFields.SorterField._id") ,col("sorterDataInterface.SorterDataInterface.sorterFields.SorterField._precision")   )) ) .select ("Transformation_Name" , col("col").cast("struct<Column_Name:string,Column_Id:string ,Column_Precision:bigint >") ). select("Transformation_Name" , col("col.Column_Name") ,  col("col.Column_Id") ,  col("col.Column_Precision") ).createOrReplaceTempView("df_sorter_details")
    spark.sql(" select A.Transformation_Name as `Sorter Name`, Column_Name as `Sort Field`, 'Ascending' Order from df_sorter_details A , df_sorter_ascn B where A.Transformation_Name = B.Transformation_Name and A.Column_Id = B.Column_Id  ").createOrReplaceTempView("df_sorter_complete")
    spark.sql(" select B.*,A.* from  df_sorter_complete A left outer join  df_mapping_name B on 1=1 ").show()
    spark.sql(" select B.*,A.* from  df_sorter_complete A left outer join  df_mapping_name B on 1=1 ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
  
    name="Sorter.csv"
    rename(output1,name)
     
    print ("Copying CSV files")
    src = output1 + name
    dst = output1 + directory +"/"+ name	
    shutil.move(src, dst)
  

  expression="expressionInterface"
  if has_column(df1,expression):
   print ("Printing Expression Transformation Details ...")
   df_mapping=spark.read.format('xml').option("rootTag", "imx:IMX").option("rowTag", "mapping:Mapping").load(sourcePath)
   df_mapping.select(col("_name").alias("Mapping Reference")).createOrReplaceTempView("df_mapping_name")
   df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)

   df2=df1.dtypes
   df3=spark.createDataFrame(df2, ['colname', 'datatype'])

   if df3.withColumn( "Ind" , when( col("datatype").like( "%_expression:string%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="expressionInterface").count()== 1:    
     df1.select(col("_name").alias("Expression Name") , explode(arrays_zip(col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._name"), col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._precision") , col ("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._expression") , col ("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._output") , col ("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._type") ))).select(col("Expression Name"), col("col").cast("struct<Port_Name:string,Precision:bigint,Expression:string , Field_Type:string , Type:string>")).select(col("Expression Name"),col("col.Port_Name"),col("col.Precision") , col("col.Expression") , col("col.Field_Type") , col("col.Type") ).where(col("col.Expression").isNotNull()).createOrReplaceTempView("df_expression_complete")

     spark.sql("select B.* ,  `Expression Name` , 'Output Field' as `Field Type` ,  Port_Name as `Field Name` , substr(Type , instr(Type,'%2F')+3 ) as type , Precision,'0' as scale , Expression as `Expression Text` from df_expression_complete A , df_mapping_name B where 1=1 and Expression  <> Port_Name ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
     name="Expression.csv"
     rename(output1,name)
   
     print ("Copying CSV files")
     src = output1 + name
     dst = output1 + directory +"/"+ name	
     shutil.move(src, dst)

     
   print("Get details for New Fields ")


   javatrans="javaInterfaces"
   if has_column(df1,javatrans):
     df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
     df1.select ( col("_name").alias("Transformation_Name"),explode (col("javaInterfaces.JavaDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,javaFields:struct<JavaField:struct<VALUE:string,groupBy:boolean,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,type:string,upper:bigint,defaultValue:struct<VALUE:string,type:string,valueLiteral:string>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") , col("col.javaFields.JavaField.name").alias("Field Name") , col("col.javaFields.JavaField.output").alias("Field Type") , col("col.javaFields.JavaField.type").alias("Type") ,col("col.javaFields.JavaField.precision").alias("Precision")  ).createOrReplaceTempView("df_java_fields")
     spark.sql("select B.* ,Transformation_Name as `Transformation Name` ,`Field Name` , Group_Name as `Field Type` ,  substr(Type , instr(Type,'%2F')+3 ) as Type , Precision, '' as Scale  from df_java_fields , df_mapping_name B where 1=1 where Group_Name='Output'").show() 
     df_java_new_fields=spark.sql("select B.* ,Transformation_Name as `Transformation Name` ,`Field Name` , Group_Name as `Field Type` ,  substr(Type , instr(Type,'%2F')+3 ) as Type , Precision, '' as Scale  from df_java_fields , df_mapping_name B where 1=1 where Group_Name='Output'")

   else:
     schema = StructType([ StructField("Mapping Reference" , StringType(), True), StructField("Transformation Name", StringType(), False) ,StructField("Field Name" , StringType(), True),StructField("Field Type" , StringType(), True),StructField("Type" , StringType(), True),StructField("Precision" , StringType(), True)  ])
     df_java_new_fields=spark.createDataFrame([], schema)

        

   sqltrans="sqlInterfaces"
   if has_column(df1,sqltrans):

     df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
     df1.select ( col("_name").alias("Transformation_Name"),explode (col("sqlInterfaces.SqlDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,sqlFields:struct<SqlField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_sqlPortNativeType:string,_type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") , explode (col("col.sqlFields.SqlField"))).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,sqlPortNativeType:string,type1:string,upper1:string>") ).select(col("Transformation_Name").alias("Transformation Name") , col("col.name1").alias("Field Name") , col("Group_Name") , col("col.type1").alias("Type") , col("col.precision1").alias("Precision")  ).createOrReplaceTempView("df_sql_fields")
     spark.sql("select B.* , `Transformation Name` , `Field Name` , Group_Name as `Field Type` , substr(Type , instr(Type,'%2F')+3 ) as Type , Precision from df_sql_fields , df_mapping_name B where 1=1 where Group_Name='Output' ").show()
     df_sql_new_fields=spark.sql("select B.* , `Transformation Name` , `Field Name` , Group_Name as `Field Type` , substr(Type , instr(Type,'%2F')+3 ) as Type , Precision from df_sql_fields , df_mapping_name B where 1=1 where Group_Name='Output' ")
   else:
     schema = StructType([ StructField("Mapping Reference" , StringType(), True), StructField("Transformation Name", StringType(), False) ,StructField("Field Name" , StringType(), True),StructField("Field Type" , StringType(), True),StructField("Type" , StringType(), True),StructField("Precision" , StringType(), True)  ])
     df_sql_new_fields=spark.createDataFrame([], schema)

 
   df_union_new_fields=df_java_new_fields.union(df_sql_new_fields)

   df_union_new_fields.coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output) 
  
   name="New Fields.csv"
   rename(output1,name)
   
   print ("Copying CSV files")
   src = output1 + name
   dst = output1 + directory +"/"+ name	
   shutil.move(src, dst)

     
    
 
   
   print ("Writing Include columns to output ................")
   #df1.select(col("_name") , explode(arrays_zip(col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._name"), col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._precision") , col ("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._expression") ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Precision:bigint,Expression:string>")).select(col("_name"),col("col.Port_Name") ).createOrReplaceTempView("df_expression")
   df2=df1.dtypes
   df3=spark.createDataFrame(df2, ['colname', 'datatype'])

   if df3.withColumn( "Ind" , when( col("datatype").like( "%_expression:string%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="expressionInterface").count()== 1:    
     df1.select(col("_name") , explode(arrays_zip( col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._name"), col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._precision") , col ("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._expression") ,col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._input")   ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Precision:bigint,Expression:string,Input:string>")).select(col("_name"),col("col.Port_Name") , col("col.Input") ).createOrReplaceTempView("df_expression")
     df_include=spark.sql("select B.`Mapping Reference` , _name as  `Transformation Name`,'Include' as `File Rules - Operator` ,'Named Fields' as  `Field Selection Criteria`,  collect_list(Port_Name) as Detail from df_expression , df_mapping_name B where 1=1 and Input='true'  group by _name , B.`Mapping Reference` ")

     df_include.withColumn("Detail_Op",col("Detail").cast("string")).withColumn("Detail_O1", regexp_replace(col('Detail_Op'), "\[", "") ).withColumn("Detail_Output", regexp_replace(col('Detail_O1'), "]", "") ).drop("Detail").drop("Detail_Op").drop("Detail_O1").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

     name="Incoming Fields.csv"
     rename(output1,name)

     print ("Copying CSV files")
     src = output1 + name
     dst = output1 + directory +"/"+ name	
     shutil.move(src, dst)
     print("Starting Transformation Lineage .........")

  df_rel = spark.read.format('xml').option("rootTag", "instances").option("rowTag", "Instance").option("charset", "UTF-8").load(sourcePath)
  df2=df_rel.dtypes
  df3=spark.createDataFrame(df2, ['colname', 'datatype'])

  if df3.withColumn( "Ind" , when( col("datatype").like( "%OutlineLink:array%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="fromOutlineLinks").count()== 1:
   print("Array Type found ....")
   df_rel.withColumn ("Transformation_Name" , col("_name")).withColumn ("From" , col("_id")).withColumn ("To" , col("fromOutlineLinks.OutlineLink._toInstance")).select("Transformation_Name","From",explode_outer("To").alias("To")).createOrReplaceTempView("df_transformation1")
  else:
   print("Structure Type found ....")
   df_rel.withColumn ("Transformation_Name" , col("_name")).withColumn ("From" , col("_id")).withColumn ("To" , col("fromOutlineLinks.OutlineLink._toInstance")).select("Transformation_Name","From","To").createOrReplaceTempView("df_transformation1")  

  df_map_transformation=spark.read.format('xml').option("rootTag", "imx:IMX").option("rowTag", "mapping:Mapping").load(sourcePath)
  df_map_transformation.withColumn("Mapping_Name", col("_name")).select(col("Mapping_Name") , explode(arrays_zip(col("instances.Instance._name"), col("instances.Instance._id") ))).select(col("Mapping_Name"), col("col").cast("struct<Transformation_Name:string,Transformation_Id:string>")).select(col("Mapping_Name"),col("col.Transformation_Name"),col("col.Transformation_Id")).createOrReplaceTempView("df_map_transformation")  
  spark.sql("Select b.Mapping_Name as Mapping_Name ,a.* from df_transformation1 a , df_map_transformation b where a.Transformation_Name = b.Transformation_Name and a.From = b.Transformation_Id").createOrReplaceTempView("df_map_transformation_joined")

  df_transformation=spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").load(sourcePath)

  df_transformation.select(col("_name").alias("Transformation_Name") , col("_type").alias("Transformation_type")  ).createOrReplaceTempView("df_transformation_dtl")

  
  spark.sql(" select A.* , substr ( B.Transformation_type ,1, instr(B.Transformation_type , ':')-1) `To Type` from ( select Current_Mapping_Name as `Mapping Name`, Current_transformation `From Transformation` , substr ( B.Transformation_type ,1, instr(B.Transformation_type , ':')-1) `From Type`, '' `From Group` , A.Next_Transformation `To Transformation`   from (    select distinct a.Mapping_Name as Current_Mapping_Name , a.Transformation_Name  Current_transformation, b.Mapping_Name as Next_Mapping_Name , b.Transformation_Name Next_Transformation  from df_map_transformation_joined a , df_map_transformation_joined b where a.To = b.From )A , df_transformation_dtl B where A.Current_transformation=B.Transformation_Name ) A , df_transformation_dtl B where A.`To Transformation`=B.Transformation_Name ").show(100,False)

  spark.sql(" select A.* , substr ( B.Transformation_type ,1, instr(B.Transformation_type , ':')-1) `To Type` from ( select Current_Mapping_Name as `Mapping Name`, Current_transformation `From Transformation` , substr ( B.Transformation_type ,1, instr(B.Transformation_type , ':')-1) `From Type`, '' `From Group` , A.Next_Transformation `To Transformation`   from (    select distinct a.Mapping_Name as Current_Mapping_Name , a.Transformation_Name  Current_transformation, b.Mapping_Name as Next_Mapping_Name , b.Transformation_Name Next_Transformation  from df_map_transformation_joined a , df_map_transformation_joined b where a.To = b.From )A , df_transformation_dtl B where A.Current_transformation=B.Transformation_Name ) A , df_transformation_dtl B where A.`To Transformation`=B.Transformation_Name ").createOrReplaceTempView("df_lin_1")  


  df_rel = spark.read.format('xml').option("rootTag", "instances").option("rowTag", "Instance").load(sourcePath)
  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").load(sourcePath)

  joiner="joinerInterfaces"
  if has_column(df1,joiner):
    df2=df1.dtypes
    df3=spark.createDataFrame(df2, ['colname', 'datatype'])
    if df3.withColumn( "Ind" , when( col("datatype").like( "%scale%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="joinerInterfaces").count()== 1:
      df_joiner=df1.select ( col("_name").alias("Transformation_Name"),explode (col("joinerInterfaces.JoinerDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<detail:boolean,id:string,input:boolean,name:string,output:boolean,type:string,joinerFields:struct<JoinerField:array<struct<VALUE:string,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,scale:bigint ,type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.joinerFields.JoinerField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,scale1:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
    else:
      df_joiner=df1.select ( col("_name").alias("Transformation_Name"),explode (col("joinerInterfaces.JoinerDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<detail:boolean,id:string,input:boolean,name:string,output:boolean,type:string,joinerFields:struct<JoinerField:array<struct<VALUE:string,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.joinerFields.JoinerField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_joiner=spark.createDataFrame([], schema) 

  filter="filterInterface"
  if has_column(df1,filter):
    df_filter=df1.select(col("_name") , explode(arrays_zip(col("filterInterface.FilterDataInterface.filterFields.FilterField._name"), col("filterInterface.FilterDataInterface.filterFields.FilterField._id")  ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Port_Id:string>")).select(col("_name").alias("Transformation_Name"),col("col.Port_Name"), col("col.Port_Id") ).withColumn("Group_Name", lit('')).select("Transformation_Name" , "Group_Name","Port_Name" , "Port_Id" )

  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_filter=spark.createDataFrame([], schema)

  
  sequence="sequenceGenInterfaces"
  if has_column(df1,sequence):
    df_sequence=df1.select(col("_name") , explode(arrays_zip(col("sequenceGenInterfaces.SequenceDataInterface.sequenceGenFields.SequenceField._name"), col("sequenceGenInterfaces.SequenceDataInterface.sequenceGenFields.SequenceField._id")  ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Port_Id:string>")).select(col("_name").alias("Transformation_Name"),col("col.Port_Name"), col("col.Port_Id") ).withColumn("Group_Name", lit('')).select("Transformation_Name" , "Group_Name","Port_Name" , "Port_Id" )

  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_sequence=spark.createDataFrame([], schema)
  
  sorter="sorterDataInterface"
  if has_column(df1,sorter):
    df_sorter=df1.select(col("_name") , explode(arrays_zip(col("sorterDataInterface.SorterDataInterface.sorterFields.SorterField._name"), col("sorterDataInterface.SorterDataInterface.sorterFields.SorterField._id")  ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Port_Id:string>")).select(col("_name").alias("Transformation_Name"),col("col.Port_Name"), col("col.Port_Id") ).withColumn("Group_Name", lit('')).select("Transformation_Name" , "Group_Name","Port_Name" , "Port_Id" )
  
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_sorter=spark.createDataFrame([], schema)
	
  expression="expressionInterface"
  if has_column(df1,expression):
    df_expression=df1.select(col("_name") , explode(arrays_zip(col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._name"), col("expressionInterface.ExpressionDataInterface.expressionFields.ExpressionField._id")  ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Port_Id:string>")).select(col("_name").alias("Transformation_Name"),col("col.Port_Name"), col("col.Port_Id") ).withColumn("Group_Name", lit('')).select("Transformation_Name" , "Group_Name","Port_Name" , "Port_Id" )
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_expression=spark.createDataFrame([], schema)


  aggregate="aggregatorInterface"
  if has_column(df1,aggregate):
    df_aggregate=df1.select(col("_name") , explode(arrays_zip(col("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._name"), col("aggregatorInterface.AggregatorDataInterface.aggregatorFields.AggregatorField._id")  ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Port_Id:string>")).select(col("_name").alias("Transformation_Name"),col("col.Port_Name"), col("col.Port_Id") ).withColumn("Group_Name", lit('')).select("Transformation_Name" , "Group_Name","Port_Name" , "Port_Id" )
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_aggregate=spark.createDataFrame([], schema)

  anonymousDso="anonymousDso"
  if has_column(df1,anonymousDso):
    df_src_target=df1.select(col("_name").alias("Transformation_Name")  ,explode(arrays_zip(col("anonymousDso.ownedCapabilityTypes.ComplexType.features.StructuralFeature._name"), col("anonymousDso.ownedCapabilityTypes.ComplexType.features.StructuralFeature._id")  ))).select( col("Transformation_Name"), col("col").cast("struct<Port_Name:string,Port_Id:string>")).select( col("Transformation_Name"),col("col.Port_Name"),col("col.Port_Id") ).withColumn("Group_Name", lit('')).select("Transformation_Name" , "Group_Name","Port_Name" , "Port_Id" )

  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_src_target=spark.createDataFrame([], schema)
 

  print("Adding Flat File Port details ......")
  df_flat = spark.read.format('xml').option("rootTag", "imx:IMX").option("rowTag", "datasource:FlatByteDataSource").load(sourcePath)
  if df_flat.count() > 0:
    #df_flat.select(col("_name").alias("Transformation_Name"), explode ( arrays_zip ( col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._name") , col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._id") ))   ).select("Transformation_Name" , col("col").cast("struct<Port_Name:string,Port_Id:string>")).select("Transformation_Name",lit("").alias("Group_Name"),col("col.Port_Name").alias("Port_Name") , col("col.Port_Id").alias("Port_Id") ).show(20,False)
    #df_flat_port=df_flat.select(col("_name").alias("Transformation_Name"), explode ( arrays_zip ( col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._name") , col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._id") ))   ).select("Transformation_Name" , col("col").cast("struct<Port_Name:string,Port_Id:string>")).select("Transformation_Name",lit("").alias("Group_Name"),col("col.Port_Name").alias("Port_Name") , col("col.Port_Id").alias("Port_Id") )
    df_flat.select(col("_name").alias("Transformation_Name"),col("defaultGenerator._id").alias("Transformation_Id"), explode ( arrays_zip ( col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._name") , col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._id") ))   ).select("Transformation_Name" ,"Transformation_Id", col("col").cast("struct<Port_Name:string,Port_Id:string>")).select("Transformation_Name","Transformation_Id",lit("").alias("Group_Name"),col("col.Port_Name").alias("Port_Name") , col("col.Port_Id").alias("Port_Id") ).show(20,False)
    df_flat.select(col("_name").alias("Transformation_Name"),col("defaultGenerator._id").alias("Transformation_Id"), explode ( arrays_zip ( col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._name") , col("defaultGenerator.ownedCapabilityTypes.ComplexType.features.StructuralFeature._id") ))   ).select("Transformation_Name" ,"Transformation_Id", col("col").cast("struct<Port_Name:string,Port_Id:string>")).select("Transformation_Name","Transformation_Id",lit("").alias("Group_Name"),col("col.Port_Name").alias("Port_Name") , col("col.Port_Id").alias("Port_Id") ).createOrReplaceTempView("df_flat")

    df_flat_abs = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").option("charset", "UTF-8").load(sourcePath)
    df_flat_abs.select(col("_name").alias("Transformation_Name") , col("_dso").alias("Transformation_Id") ).createOrReplaceTempView("df_flat_abs")
    
    spark.sql("select B.Transformation_Name , Group_Name , Port_Name , Port_Id from df_flat A, df_flat_abs B where A.Transformation_Id=B.Transformation_Id").show()
    df_flat_port=spark.sql("select B.Transformation_Name , Group_Name , Port_Name , Port_Id from df_flat A, df_flat_abs B where A.Transformation_Id=B.Transformation_Id")
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_flat_port=spark.createDataFrame([], schema)



  router="routerInterfaces"
  if has_column(df1,router):
    df_type=df1.dtypes
    df3_type=spark.createDataFrame(df_type  , ['colname', 'datatype'])
    if df3_type.filter(col("colname").like("%routerInterfaces%")).filter(col("datatype").like("%annotations%")).count()==1 :
      print("Annotations tag found in Router ....")
      if df3_type.filter(col("colname").like("%routerInterfaces%")).filter(col("datatype").like("%body%")).count()==1 :
        print("Body tag found under Router tag ......")
        if df3_type.filter(col("colname").like("%routerInterfaces%")).filter(col("datatype").like("%scale%")).count()==1 :
          print("Scale tag found ...........")
          if df3_type.filter(col("colname").like("%routerInterfaces%")).filter(col("datatype").like("%defaultValue%")).count()==1 :
            df_router1= df1.select ( col("_name").alias("Transformation_Name"),explode (col("routerInterfaces.RouterDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<dataCondition:string,id:string,input:boolean,name:string,output:boolean,type:string,routerFields:struct<RouterField:array<struct<VALUE:string,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,scale:bigint,type:string,upper:bigint,annotations:struct<Annotation:struct<VALUE:string,body:string,id:string,type:string>>,defaultValue:struct<_VALUE:string,_type:string,_valueLiteral:string>>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.routerFields.RouterField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,scale:bigint,type1:string,upper1:string,annotations:struct<Annotation:struct<VALUE:string,body:string,id:string,type:string>>,defaultValue:struct<VALUE1:string,type1:string,valueLiteral1:string>>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id")) 
          else:
            df_router1=df1.select ( col("_name").alias("Transformation_Name"),explode (col("routerInterfaces.RouterDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<dataCondition:string,id:string,input:boolean,name:string,output:boolean,type:string,annotations:struct<Annotation:struct<VALUE:string,body:string,id:string,type:string>>,routerFields:struct<RouterField:array<struct<VALUE:string,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,scale:bigint,type:string,upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.routerFields.RouterField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,scale:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))            
        else:
          df_router1=df1.select ( col("_name").alias("Transformation_Name"),explode (col("routerInterfaces.RouterDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<dataCondition:string,id:string,input:boolean,name:string,output:boolean,type:string,annotations:struct<Annotation:struct<VALUE:string,body:string,id:string,type:string>>,routerFields:struct<RouterField:array<struct<VALUE:string,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,type:string,upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.routerFields.RouterField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id")) 
      else:
        df_router1=df1.select ( col("_name").alias("Transformation_Name"),explode (col("routerInterfaces.RouterDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<dataCondition:string,id:string,input:boolean,name:string,output:boolean,type:string,annotations:struct<Annotation:struct<VALUE:string,id:string,type:string>>,routerFields:struct<RouterField:array<struct<VALUE:string,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,scale:bigint,type:string,upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.routerFields.RouterField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,scale:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))        
    else:
      print("Annotations tag not found in Router ....")
      if df3_type.filter(col("colname").like("%routerInterfaces%")).filter(col("datatype").like("%defaultValue%")).count()==1 :
       df_router1=df1.select ( col("_name").alias("Transformation_Name"),explode (col("routerInterfaces.RouterDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<dataCondition:string,id:string,input:boolean,name:string,output:boolean,type:string,routerFields:struct<RouterField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_scale:bigint,_type:string,_upper:bigint,defaultValue:struct<_VALUE:string,_type:string,_valueLiteral:string>>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.routerFields.RouterField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,scale1:bigint,type1:string,upper1:string,defaultValue1:struct<VALUE1:string,type1:string,valueLiteral1:string>>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
      else: 
       df_router1=df1.select ( col("_name").alias("Transformation_Name"),explode (col("routerInterfaces.RouterDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<dataCondition:string,id:string,input:boolean,name:string,output:boolean,type:string,routerFields:struct<RouterField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_scale:bigint,_type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.routerFields.RouterField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,scale:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
       
    #df_router1=df1.select ( col("_name").alias("Transformation_Name"),explode (col("routerInterfaces.RouterDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<dataCondition:string,id:string,input:boolean,name:string,output:boolean,type:string,annotations:struct<Annotation:struct<VALUE:string,id:string,type:string>>,routerFields:struct<RouterField:array<struct<VALUE:string,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,type:string,upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.routerFields.RouterField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
    
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_router1=spark.createDataFrame([], schema)


  union="unionInterfaces" 
  if has_column(df1,union):
    df2=df1.dtypes
    df3=spark.createDataFrame(df2, ['colname', 'datatype'])
    if df3.withColumn( "Ind" , when( col("datatype").like( "%scale%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="unionInterfaces").count()== 1:
      df_union1 = df1.select ( col("_name").alias("Transformation_Name"),explode (col("unionInterfaces.UnionDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,unionFields:struct<UnionField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_scale:bigint ,_type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.unionFields.UnionField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,scale1:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))  
    else:
      df_union1 = df1.select ( col("_name").alias("Transformation_Name"),explode (col("unionInterfaces.UnionDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,unionFields:struct<UnionField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.unionFields.UnionField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_union1=spark.createDataFrame([], schema)
  
  
  '''
  union="unionInterfaces" 
  if has_column(df1,union):
    df_union1 = df1.select ( col("_name").alias("Transformation_Name"),explode (col("unionInterfaces.UnionDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,unionFields:struct<UnionField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") ,explode (col("col.unionFields.UnionField"))  ).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,type1:string,upper1:string>") ).select("Transformation_Name" , "Group_Name" , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_union1=spark.createDataFrame([], schema)
  '''

  update="updateStrategyInterface"
  if has_column(df1,update):
    df_update=df1.select(col("_name") , explode(arrays_zip(col("updateStrategyInterface.UpdateStrategyDataInterface.updateStrategyFields.UpdateStrategyField._name"), col("updateStrategyInterface.UpdateStrategyDataInterface.updateStrategyFields.UpdateStrategyField._id")  ))).select(col("_name"), col("col").cast("struct<Port_Name:string,Port_Id:string>")).select(col("_name").alias("Transformation_Name"),col("col.Port_Name"), col("col.Port_Id") ).withColumn("Group_Name", lit('')).select("Transformation_Name" , "Group_Name","Port_Name" , "Port_Id" )

  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_update=spark.createDataFrame([], schema)


  sqlint="sqlInterfaces" 
  if has_column(df1,sqlint):
    df_sql_port = df1.select ( col("_name").alias("Transformation_Name"),explode (col("sqlInterfaces.SqlDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,sqlFields:struct<SqlField:array<struct<_VALUE:string,_id:string,_input:boolean,_lower:bigint,_name:string,_output:boolean,_precision:bigint,_sqlPortNativeType:string,_type:string,_upper:bigint>>>>")).select(col("Transformation_Name") , col("col.name").alias("Group_Name") , explode (col("col.sqlFields.SqlField"))).select(col("Transformation_Name") , col("Group_Name") , col("col").cast("struct<VALUE1:string,id1:string,input1:boolean,lower1:bigint,name1:string,output1:boolean,precision1:bigint,sqlPortNativeType:string,type1:string,upper1:string>") ).select(col("Transformation_Name").alias("Transformation Name") ,col("Group_Name").alias("Group_Name") , col("col.name1").alias("Port_Name") , col("col.id1").alias("Port_Id"))
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_sql_port=spark.createDataFrame([], schema)


  javaInterface="javaInterfaces"
  if has_column(df1,javaInterface):
    df_java_port=df1.select ( col("_name").alias("Transformation_Name"),explode (col("javaInterfaces.JavaDataInterface")) ).select(col("Transformation_Name") , col("col").cast("struct<id:string,input:boolean,name:string,output:boolean,type:string,javaFields:struct<JavaField:struct<VALUE:string,groupBy:boolean,id:string,input:boolean,lower:bigint,name:string,output:boolean,precision:bigint,type:string,upper:bigint,defaultValue:struct<VALUE:string,type:string,valueLiteral:string>>>>")).select(col("Transformation_Name")  ,col("col.name").alias("Group_Name") , col("col.javaFields.JavaField.name").alias("Port_Name") ,col("col.javaFields.JavaField.id").alias("Port_Id")  )
  else:
    schema = StructType([     StructField("Transformation_Name" , StringType(), True), StructField("Group_Name", StringType(), False) , StructField("Port_Name", StringType(), False) , StructField("Port_Id", StringType(), False)  ])
    df_java_port=spark.createDataFrame([], schema)

  
  df_union=df_joiner.union(df_sorter).union(df_filter).union(df_expression).union(df_aggregate).union(df_src_target).union(df_union1).union(df_router1).union(df_sequence).union(df_update).union(df_sql_port).union(df_java_port).union(df_flat_port)
  df_union.createOrReplaceTempView("df_union")
  output_tmp=output+"/tmp"
  df_union.coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output_tmp)
  df_complete = spark.read.csv(output_tmp,header=True)
  df_complete.createOrReplaceTempView("df_complete")
  print("Getting first level of lineage ......")

  spark.sql("select * from df_complete ").show()

  df_port_lineage_1=df_rel.withColumn ("Transformation_Name" , col("_name")).select("Transformation_Name" , explode(arrays_zip(col("ports.TransformationFieldPort._id"), col("ports.TransformationFieldPort._fromPort") , col("ports.TransformationFieldPort._transformationField") , col("ports.TransformationFieldPort._toPorts") ))  ).select ("Transformation_Name" , col("col").cast("struct<Port_id:string,From_Port:string,Port_Instance:string,To_Port:string>")).select(col("Transformation_Name") , col("col.Port_id") , col("col.From_Port") , col("col.Port_Instance") , col("col.To_Port") )
  df2=df_rel.dtypes
  df3=spark.createDataFrame(df2, ['colname', 'datatype'])
  if df3.withColumn( "Ind" , when( col("datatype").like( "%NestedPort:struct%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="ports").count()== 1:
    if df3.withColumn( "Ind" , when( col("datatype").like( "%_structuralFeature:string,_toPorts:string%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="ports").count()== 1:
      df_port_lineage_2=df_rel.select( col("_name").alias("Transformation_Name"),  explode(col("ports.TransformationFieldPort.children.NestedPort"))).select(col("Transformation_Name") , col("col").cast("struct<VALUE:string,fromPort:string,id:string,parent:string,structuralFeature:string,toPorts:string,_type:string>")   ).select(col("Transformation_Name") ,col("col.id").alias("Port_id"), col("col.fromPort").alias("From_Port") , col("col.structuralFeature").alias("Port_Instance") ,col("col.toPorts").alias("To_Port"))
    else:
      df_port_lineage_2=df_rel.select( col("_name").alias("Transformation_Name"),  explode(col("ports.TransformationFieldPort.children.NestedPort"))).select(col("Transformation_Name") , col("col").cast("struct<VALUE:string,fromPort:string,id:string,parent:string,structuralFeature:string,_type:string>")   ).select(col("Transformation_Name") ,col("col.id").alias("Port_id"), col("col.fromPort").alias("From_Port") , col("col.structuralFeature").alias("Port_Instance") ,lit("").alias("To_Port"))
  else:
    if df3.withColumn( "Ind" , when( col("datatype").like( "%_structuralFeature:string,_toPorts:string%"),"Array1" ).otherwise("NotFound")).filter(col("Ind")=="Array1").where(col("colname")=="ports").count()== 1:
      print("Array type found and toPorts element found ....")
      df_port_lineage_2=df_rel.select( col("_name").alias("Transformation_Name"),  explode(col("ports.TransformationFieldPort.children.NestedPort"))).select(col("Transformation_Name") , col("col").cast("array<struct<VALUE:string,fromPort:string,id:string,parent:string,structuralFeature:string,toPorts:string,_type:string>>")   ).select(col("Transformation_Name") ,explode(arrays_zip(col("col.id"), col("col.fromPort") , col("col.structuralFeature") ,col("col.toPorts") ))  ).select(col("Transformation_Name") , col("col").cast("struct<Port_id:string,From_Port:string , Port_Instance:string , To_Port:string >")).select(col("Transformation_Name") , col("col.Port_id") , col("col.From_Port") , col("col.Port_Instance") , col("col.To_Port") )
    else:
      print("Array type found but toPorts element not found ....")
      df_port_lineage_2=df_rel.select( col("_name").alias("Transformation_Name"),  explode(col("ports.TransformationFieldPort.children.NestedPort"))).select(col("Transformation_Name") , col("col").cast("array<struct<VALUE:string,fromPort:string,id:string,parent:string,structuralFeature:string,_type:string>>")   ).select(col("Transformation_Name") ,explode(arrays_zip(col("col.id"), col("col.fromPort") , col("col.structuralFeature")  ))  ).select(col("Transformation_Name") , col("col").cast("struct<Port_id:string,From_Port:string , Port_Instance:string >")).select(col("Transformation_Name") , col("col.Port_id") , col("col.From_Port") , col("col.Port_Instance") ,lit("").alias("To_Port") )
  df_port_lineage=df_port_lineage_1.union(df_port_lineage_2)
  df_port_lineage.createOrReplaceTempView("df_port_lineage")

  spark.sql(" select a.Transformation_Name , b.Port_Name ,b.Group_Name as Group_Name ,  a.Port_id ,a.From_Port , a.To_Port from df_port_lineage a , df_complete b where a.Transformation_Name = b.Transformation_Name and a.Port_Instance=b.Port_id").createOrReplaceTempView("df_port_lineage_complete") 
  spark.sql("select a.Transformation_Name as Current_Transformation, a.Port_Name as Current_Port_Name,a.Group_Name as Current_Group_Name ,  b.Transformation_Name as From_Transformation_Name , b.Port_Name as From_Port_Name , b.Group_Name as From_Group_Name , c.Transformation_Name as To_Transformation_Name , c.Port_Name To_Port_Name ,c.Group_Name as To_Group_Name  from df_port_lineage_complete a left outer join   (select Transformation_Name , Port_Name , Group_Name , Port_id from df_port_lineage_complete ) b on a.From_Port=b.Port_id left outer join   (select Transformation_Name , Port_Name ,Group_Name, Port_id from df_port_lineage_complete ) c on a.To_Port=c.Port_id      order by 1 ").show()
  spark.sql("select a.Transformation_Name as Current_Transformation, a.Port_Name as Current_Port_Name,a.Group_Name as Current_Group_Name ,  b.Transformation_Name as From_Transformation_Name , b.Port_Name as From_Port_Name , b.Group_Name as From_Group_Name , c.Transformation_Name as To_Transformation_Name , c.Port_Name To_Port_Name ,c.Group_Name as To_Group_Name  from df_port_lineage_complete a left outer join   (select Transformation_Name , Port_Name , Group_Name , Port_id from df_port_lineage_complete ) b on a.From_Port=b.Port_id left outer join   (select Transformation_Name , Port_Name ,Group_Name, Port_id from df_port_lineage_complete ) c on a.To_Port=c.Port_id      order by 1 ").createOrReplaceTempView("df_complete_port_lineage")

  print("Started writing target level field mapping ..........................")
  df1 = spark.read.format('xml').option("rootTag", "transformations").option("rowTag", "AbstractTransformation").load(sourcePath)
  df1.select(col("_name").alias("Transformation_Name") ,col("_type").alias("Transformation_Type") ).createOrReplaceTempView("df_dtl")

  spark.sql("select Transformation_Name  from df_dtl where substr(Transformation_Type ,1, instr(Transformation_Type,':')-1)='target'").createOrReplaceTempView("df_dtl1")
  spark.sql(" select To_Transformation_Name , 'Manual' as `Field Map Option`, collect_list(concat(Current_Port_Name,'->',To_Port_Name)) as `Field Map` from df_complete_port_lineage A , df_dtl1 B  where A.To_Transformation_Name = B.Transformation_Name group by To_Transformation_Name ").createOrReplaceTempView("df_dtl2")

  df_mapping=spark.read.format('xml').option("rootTag", "imx:IMX").option("rowTag", "mapping:Mapping").load(sourcePath)
  df_mapping.select(col("_name").alias("Mapping Reference")).createOrReplaceTempView("df_mapping_name")
  print("Output to target level mapping details ...........")
  spark.sql("select B.`Mapping Reference` , A.* from df_dtl2 A , df_mapping_name B where 1=1").show(10,False)
  spark.sql("select B.`Mapping Reference` ,A.To_Transformation_Name as `Target Name` , A.`Field Map Option`, concat_ws(',',A.`Field Map`) as `Field_Map` from df_dtl2 A , df_mapping_name B where 1=1").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  name="Target Field Mapping.csv"
  rename(output1,name)
  
  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)


  
  print ("Started Group level lineage writing .....")

  spark.sql("select distinct From_Transformation_Name , From_Group_Name , Current_Transformation , Current_Group_Name from df_complete_port_lineage where From_Transformation_Name is not null  ").createOrReplaceTempView("df_trans_level_lineage")
  spark.sql("select Transformation_Name , substr(Transformation_Type ,1, instr(Transformation_Type,':')-1) as Transformation_Type from df_dtl ").createOrReplaceTempView("df_trans_type")
  spark.sql(" select A.From_Transformation_Name `From Transformation` ,B.Transformation_Type as `From Type` , A.From_Group_Name as `From Group` , A.Current_Transformation `To Transformation` , C.Transformation_Type as `To Type` , A.Current_Group_Name as `To Group` from df_trans_level_lineage A left outer join df_trans_type B on A.From_Transformation_Name = B.Transformation_Name left outer join df_trans_type C on A.Current_Transformation = C.Transformation_Name  ").createOrReplaceTempView("df_trans_group_lineage")

  ##Commenting previous lineage write
  print("Current transformation lineage ....")
  spark.sql("select B.`Mapping Reference` , A.* from df_trans_group_lineage A , df_mapping_name B where 1=1").show()
  spark.sql("select B.`Mapping Reference` , A.* from df_trans_group_lineage A , df_mapping_name B where 1=1").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)

  ##Adding if less number of lineage shows from df_complete

  #spark.sql(" select A.`Mapping Name` , A.`From Transformation`,A.`From Type`,B.`From Group`,   A.`To Transformation`, A.`To Type`, B.`To Group` from df_lin_1 A left outer join (  select B.`Mapping Reference` , A.* from df_trans_group_lineage A , df_mapping_name B where 1=1 ) B on A.`From Transformation` =B.`From Transformation` and A.`From Type` = B.`From Type`   ").show()
  #spark.sql(" select A.`Mapping Name` , A.`From Transformation`,A.`From Type`,B.`From Group`,   A.`To Transformation`, A.`To Type`, B.`To Group` from df_lin_1 A left outer join (  select B.`Mapping Reference` , A.* from df_trans_group_lineage A , df_mapping_name B where 1=1 ) B on A.`From Transformation` =B.`From Transformation` and A.`From Type` = B.`From Type` ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)


  name="Transformation Lineage.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)


  print ("......Port rename level lineage .....")

  spark.sql("select From_Transformation_Name , From_Group_Name , Current_Transformation , Current_Group_Name , collect_list(concat(From_Port_Name,'->',Current_Port_Name)) as Port_trans  from df_complete_port_lineage where From_Port_Name <> Current_Port_Name group by From_Transformation_Name , From_Group_Name , Current_Transformation , Current_Group_Name  ").createOrReplaceTempView("df_port_rename")
  spark.sql(" select A.From_Transformation_Name `From Transformation` ,B.Transformation_Type as `From Type` , A.From_Group_Name as `From Group` , A.Current_Transformation `To Transformation` , C.Transformation_Type as `To Type` , A.Current_Group_Name as `To Group` , A.Port_trans  from df_port_rename A left outer join df_trans_type B on A.From_Transformation_Name = B.Transformation_Name left outer join df_trans_type C on A.Current_Transformation = C.Transformation_Name  ").createOrReplaceTempView("df_port_rename_lineage")
  spark.sql("select B.`Mapping Reference` , A.* from df_port_rename_lineage A , df_mapping_name B where 1=1").createOrReplaceTempView("df_port_rename_1")
  spark.sql("select `Mapping Reference` , `From Transformation` , `From Type` , `From Group` , `To Transformation` , `To Type` , `To Group` ,concat_ws(',',`Port_trans`) as Port_trans from df_port_rename_1 where `To Type` <> 'target' ").coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(output)
 

  name="Ports Rename Details.csv"
  rename(output1,name)

  print ("Copying CSV files")
  src = output1 + name
  dst = output1 + directory +"/"+ name	
  shutil.move(src, dst)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  #cmd4="mv " + path + "*.csv" + " "  + path + "Complete/"
  #os.system(cmd4)

  print("Generating excel output .....")
  path_complete=output1+"Complete/" 
  filename=path_complete+object_name+"_INFA_XML.xls"
  
  create_Excel(path_complete, filename)

  print ("Exiting the script .....") 
