import sys
from awsglue.transforms import 
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node airport_dim
airport_dim_node1712906107526 = glueContext.create_dynamic_frame.from_catalog(database=airline_datamart, table_name=dev_airlines_airports_dim, redshift_tmp_dir=s3redshift-temp-data-smtemp-dataairline-dim, transformation_ctx=airport_dim_node1712906107526)

# Script generated for node daily_flight_data
daily_flight_data_node1712905538493 = glueContext.create_dynamic_frame.from_catalog(database=airline_datamart, table_name=daily_flights, transformation_ctx=daily_flight_data_node1712905538493)

# Script generated for node Filter
Filter_node1712905951651 = Filter.apply(frame=daily_flight_data_node1712905538493, f=lambda row (row[depdelay] = 60), transformation_ctx=Filter_node1712905951651)

# Script generated for node Join_for_dept_airport
Filter_node1712905951651DF = Filter_node1712905951651.toDF()
airport_dim_node1712906107526DF = airport_dim_node1712906107526.toDF()
Join_for_dept_airport_node1712906279728 = DynamicFrame.fromDF(Filter_node1712905951651DF.join(airport_dim_node1712906107526DF, (Filter_node1712905951651DF['originairportid'] == airport_dim_node1712906107526DF['airport_id']), left), glueContext, Join_for_dept_airport_node1712906279728)

# Script generated for node modify_dept_airport_column
modify_dept_airport_column_node1712906557299 = ApplyMapping.apply(frame=Join_for_dept_airport_node1712906279728, mappings=[(depdelay, long, dep_delay, bigint), (arrdelay, long, arr_delay, bigint), (destairportid, long, destairportid, long), (carrier, string, carrier, string), (city, string, dep_city, string), (name, string, dep_airport, string), (state, string, dep_state, string)], transformation_ctx=modify_dept_airport_column_node1712906557299)

# Script generated for node Join_for_arrv_airport
modify_dept_airport_column_node1712906557299DF = modify_dept_airport_column_node1712906557299.toDF()
airport_dim_node1712906107526DF = airport_dim_node1712906107526.toDF()
Join_for_arrv_airport_node1712906838383 = DynamicFrame.fromDF(modify_dept_airport_column_node1712906557299DF.join(airport_dim_node1712906107526DF, (modify_dept_airport_column_node1712906557299DF['destairportid'] == airport_dim_node1712906107526DF['airport_id']), left), glueContext, Join_for_arrv_airport_node1712906838383)

# Script generated for node modify_arr_airport_columns
modify_arr_airport_columns_node1712906962998 = ApplyMapping.apply(frame=Join_for_arrv_airport_node1712906838383, mappings=[(carrier, string, carrier, string), (dep_state, string, dep_state, string), (state, string, arr_state, string), (arr_delay, bigint, arr_delay, long), (city, string, arr_city, string), (name, string, arr_airport, string), (dep_city, string, dep_city, string), (dep_delay, bigint, dep_delay, long), (dep_airport, string, dep_airport, string)], transformation_ctx=modify_arr_airport_columns_node1712906962998)

# Script generated for node redshift_target_table
redshift_target_table_node1712907141035 = glueContext.write_dynamic_frame.from_catalog(frame=modify_arr_airport_columns_node1712906962998, database=airline_datamart, table_name=dev_airlines_daily_flights_fact, redshift_tmp_dir=s3redshift-temp-data-smtemp-dataairline-fact,additional_options={aws_iam_role arnawsiam112207118291roleredshift_role}, transformation_ctx=redshift_target_table_node1712907141035)

job.commit()