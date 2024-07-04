import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1720039541890 = glueContext.create_dynamic_frame.from_catalog(database="test-glue", table_name="demo_teams_attack_2024_05_21_csv", transformation_ctx="AmazonS3_node1720039541890")

# Script generated for node Amazon Redshift
AmazonRedshift_node1720039563380 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1720039541890, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.attacks USING public.attacks_temp_7sl3do ON attacks.team_id = attacks_temp_7sl3do.team_id AND attacks.season = attacks_temp_7sl3do.season WHEN MATCHED THEN UPDATE SET team_id = attacks_temp_7sl3do.team_id, season = attacks_temp_7sl3do.season, goals = attacks_temp_7sl3do.goals, goalspermatch = attacks_temp_7sl3do.goalspermatch, shots = attacks_temp_7sl3do.shots, shotsontarget = attacks_temp_7sl3do.shotsontarget, shootingaccuracy% = attacks_temp_7sl3do.shootingaccuracy%, penaltiesscored = attacks_temp_7sl3do.penaltiesscored, bigchancescreated = attacks_temp_7sl3do.bigchancescreated, hitwoodwork = attacks_temp_7sl3do.hitwoodwork WHEN NOT MATCHED THEN INSERT VALUES (attacks_temp_7sl3do.team_id, attacks_temp_7sl3do.season, attacks_temp_7sl3do.goals, attacks_temp_7sl3do.goalspermatch, attacks_temp_7sl3do.shots, attacks_temp_7sl3do.shotsontarget, attacks_temp_7sl3do.shootingaccuracy%, attacks_temp_7sl3do.penaltiesscored, attacks_temp_7sl3do.bigchancescreated, attacks_temp_7sl3do.hitwoodwork); DROP TABLE public.attacks_temp_7sl3do; END;", "redshiftTmpDir": "s3://aws-glue-assets-211125327538-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.attacks_temp_7sl3do", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.attacks (team_id BIGINT, season VARCHAR, goals BIGINT, goalspermatch DOUBLE PRECISION, shots BIGINT, shotsontarget BIGINT, shootingaccuracy% VARCHAR, penaltiesscored BIGINT, bigchancescreated BIGINT, hitwoodwork BIGINT); DROP TABLE IF EXISTS public.attacks_temp_7sl3do; CREATE TABLE public.attacks_temp_7sl3do (team_id BIGINT, season VARCHAR, goals BIGINT, goalspermatch DOUBLE PRECISION, shots BIGINT, shotsontarget BIGINT, shootingaccuracy% VARCHAR, penaltiesscored BIGINT, bigchancescreated BIGINT, hitwoodwork BIGINT);"}, transformation_ctx="AmazonRedshift_node1720039563380")

job.commit()
