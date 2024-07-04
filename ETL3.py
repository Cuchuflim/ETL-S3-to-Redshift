import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node stats
stats_node1720108233519 = glueContext.create_dynamic_frame.from_catalog(database="test-glue", table_name="demo_teams_stats_2024_05_21_csv", transformation_ctx="stats_node1720108233519")

# Script generated for node attack
attack_node1720107630990 = glueContext.create_dynamic_frame.from_catalog(database="test-glue", table_name="demo_teams_attack_2024_05_21_csv", transformation_ctx="attack_node1720107630990")

# Script generated for node discipline
discipline_node1720107633206 = glueContext.create_dynamic_frame.from_catalog(database="test-glue", table_name="demo_teams_discipline_2024_05_21_csv", transformation_ctx="discipline_node1720107633206")

# Script generated for node Amazon S3
AmazonS3_node1720109428486 = glueContext.create_dynamic_frame.from_catalog(database="test-glue", table_name="demo_teams_overview_2024_05_21_csv", transformation_ctx="AmazonS3_node1720109428486")

# Script generated for node defence
defence_node1720107632744 = glueContext.create_dynamic_frame.from_catalog(database="test-glue", table_name="demo_teams_defence_2024_05_21_csv", transformation_ctx="defence_node1720107632744")

# Script generated for node teamplay
teamplay_node1720108234684 = glueContext.create_dynamic_frame.from_catalog(database="test-glue", table_name="demo_teams_teamplay_2024_05_21_csv", transformation_ctx="teamplay_node1720108234684")

# Script generated for node Change Schema
ChangeSchema_node1720109440092 = ApplyMapping.apply(frame=AmazonS3_node1720109428486, mappings=[("clubbadge", "string", "clubbadge", "varchar"), ("homekit", "string", "homekit", "varchar"), ("awaykit", "string", "awaykit", "varchar"), ("thirdkit", "string", "thirdkit", "varchar"), ("team_id", "long", "team_id", "int"), ("teamname", "string", "teamname", "varchar")], transformation_ctx="ChangeSchema_node1720109440092")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT di.*, de.*, att.*, st.*, te.*
FROM discipline AS di 
JOIN 
    defence AS de ON di.team_id = de.team_id AND di.season = de.season
JOIN 
    attack AS att ON di.team_id = att.team_id AND di.season = att.season
JOIN
    stats AS st ON di.team_id = st.team_id AND di.season = st.season
JOIN
    teamplay AS te ON di.team_id = te.team_id AND di.season = te.season;
'''
SQLQuery_node1720107710224 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"discipline":discipline_node1720107633206, "defence":defence_node1720107632744, "attack":attack_node1720107630990, "stats":stats_node1720108233519, "teamplay":teamplay_node1720108234684}, transformation_ctx = "SQLQuery_node1720107710224")

# Script generated for node Change Schema
ChangeSchema_node1720108948681 = ApplyMapping.apply(frame=SQLQuery_node1720107710224, mappings=[("team_id", "bigint", "team_id", "int"), ("season", "string", "season", "varchar"), ("yellowcards", "bigint", "yellowcards", "int"), ("redcards", "bigint", "redcards", "int"), ("fouls", "bigint", "fouls", "int"), ("offsides", "bigint", "offsides", "int"), ("cleansheets", "bigint", "cleansheets", "int"), ("goalconceded", "bigint", "goalconceded", "int"), ("goalsconcededpermatch", "double", "goalsconcededpermatch", "decimal"), ("saves", "bigint", "saves", "int"), ("tackle", "bigint", "tackle", "int"), ("tacklesuccess%", "string", "tacklesuccess", "varchar"), ("blockedshots", "bigint", "blockedshots", "int"), ("interceptions", "bigint", "interceptions", "int"), ("clearances", "string", "clearances", "varchar"), ("headedclearance", "bigint", "headedclearance", "int"), ("aerialbattle/duelswon", "string", "aerialbattleduelswon", "varchar"), ("errorsleadingtogoal", "bigint", "errorsleadingtogoal", "int"), ("owngoals", "bigint", "owngoals", "int"), ("goals", "bigint", "goals", "int"), ("goalspermatch", "decimal", "goalspermatch", "decimal"), ("shots", "int", "shots", "int"), ("shotsontarget", "int", "shotsontarget", "int"), ("shootingaccuracy", "string", "shootingaccuracy", "varchar"), ("penaltiesscored", "int", "penaltiesscored", "int"), ("bigchancescreated", "int", "bigchancescreated", "int"), ("hitwoodwork", "int", "hitwoodwork", "int"), ("teamname", "string", "teamname", "varchar"), ("matches_played", "bigint", "matches_played", "int"), ("wins", "bigint", "wins", "int"), ("losses", "bigint", "losses", "int"), ("goal_conceded", "bigint", "goal_conceded", "long"), ("clean_sheet", "bigint", "clean_sheet", "long"), ("passes", "string", "passes", "varchar"), ("passespermatch", "double", "passespermatch", "decimal"), ("passaccuracy%", "string", "passaccuracy", "varchar"), ("crosses", "bigint", "crosses", "int"), ("crossaccuracy%", "string", "crossaccuracy", "varchar")], transformation_ctx="ChangeSchema_node1720108948681")

# Script generated for node Amazon Redshift
AmazonRedshift_node1720109447050 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1720109440092, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.overview USING public.overview_temp_s8plo2 ON overview.team_id = overview_temp_s8plo2.team_id WHEN MATCHED THEN UPDATE SET clubbadge = overview_temp_s8plo2.clubbadge, homekit = overview_temp_s8plo2.homekit, awaykit = overview_temp_s8plo2.awaykit, thirdkit = overview_temp_s8plo2.thirdkit, team_id = overview_temp_s8plo2.team_id, teamname = overview_temp_s8plo2.teamname WHEN NOT MATCHED THEN INSERT VALUES (overview_temp_s8plo2.clubbadge, overview_temp_s8plo2.homekit, overview_temp_s8plo2.awaykit, overview_temp_s8plo2.thirdkit, overview_temp_s8plo2.team_id, overview_temp_s8plo2.teamname); DROP TABLE public.overview_temp_s8plo2; END;", "redshiftTmpDir": "s3://aws-glue-assets-211125327538-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.overview_temp_s8plo2", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.overview (clubbadge VARCHAR, homekit VARCHAR, awaykit VARCHAR, thirdkit VARCHAR, team_id INTEGER, teamname VARCHAR); DROP TABLE IF EXISTS public.overview_temp_s8plo2; CREATE TABLE public.overview_temp_s8plo2 (clubbadge VARCHAR, homekit VARCHAR, awaykit VARCHAR, thirdkit VARCHAR, team_id INTEGER, teamname VARCHAR);"}, transformation_ctx="AmazonRedshift_node1720109447050")

# Script generated for node Amazon Redshift
AmazonRedshift_node1720109369223 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1720108948681, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.stats USING public.stats_temp_pm073h ON stats.team_id = stats_temp_pm073h.team_id AND stats.season = stats_temp_pm073h.season WHEN MATCHED THEN UPDATE SET team_id = stats_temp_pm073h.team_id, season = stats_temp_pm073h.season, yellowcards = stats_temp_pm073h.yellowcards, redcards = stats_temp_pm073h.redcards, fouls = stats_temp_pm073h.fouls, offsides = stats_temp_pm073h.offsides, cleansheets = stats_temp_pm073h.cleansheets, goalconceded = stats_temp_pm073h.goalconceded, goalsconcededpermatch = stats_temp_pm073h.goalsconcededpermatch, saves = stats_temp_pm073h.saves, tackle = stats_temp_pm073h.tackle, tacklesuccess = stats_temp_pm073h.tacklesuccess, blockedshots = stats_temp_pm073h.blockedshots, interceptions = stats_temp_pm073h.interceptions, clearances = stats_temp_pm073h.clearances, headedclearance = stats_temp_pm073h.headedclearance, aerialbattleduelswon = stats_temp_pm073h.aerialbattleduelswon, errorsleadingtogoal = stats_temp_pm073h.errorsleadingtogoal, owngoals = stats_temp_pm073h.owngoals, goals = stats_temp_pm073h.goals, goalspermatch = stats_temp_pm073h.goalspermatch, shots = stats_temp_pm073h.shots, shotsontarget = stats_temp_pm073h.shotsontarget, shootingaccuracy = stats_temp_pm073h.shootingaccuracy, penaltiesscored = stats_temp_pm073h.penaltiesscored, bigchancescreated = stats_temp_pm073h.bigchancescreated, hitwoodwork = stats_temp_pm073h.hitwoodwork, teamname = stats_temp_pm073h.teamname, matches_played = stats_temp_pm073h.matches_played, wins = stats_temp_pm073h.wins, losses = stats_temp_pm073h.losses, goal_conceded = stats_temp_pm073h.goal_conceded, clean_sheet = stats_temp_pm073h.clean_sheet, passes = stats_temp_pm073h.passes, passespermatch = stats_temp_pm073h.passespermatch, passaccuracy = stats_temp_pm073h.passaccuracy, crosses = stats_temp_pm073h.crosses, crossaccuracy = stats_temp_pm073h.crossaccuracy WHEN NOT MATCHED THEN INSERT VALUES (stats_temp_pm073h.team_id, stats_temp_pm073h.season, stats_temp_pm073h.yellowcards, stats_temp_pm073h.redcards, stats_temp_pm073h.fouls, stats_temp_pm073h.offsides, stats_temp_pm073h.cleansheets, stats_temp_pm073h.goalconceded, stats_temp_pm073h.goalsconcededpermatch, stats_temp_pm073h.saves, stats_temp_pm073h.tackle, stats_temp_pm073h.tacklesuccess, stats_temp_pm073h.blockedshots, stats_temp_pm073h.interceptions, stats_temp_pm073h.clearances, stats_temp_pm073h.headedclearance, stats_temp_pm073h.aerialbattleduelswon, stats_temp_pm073h.errorsleadingtogoal, stats_temp_pm073h.owngoals, stats_temp_pm073h.goals, stats_temp_pm073h.goalspermatch, stats_temp_pm073h.shots, stats_temp_pm073h.shotsontarget, stats_temp_pm073h.shootingaccuracy, stats_temp_pm073h.penaltiesscored, stats_temp_pm073h.bigchancescreated, stats_temp_pm073h.hitwoodwork, stats_temp_pm073h.teamname, stats_temp_pm073h.matches_played, stats_temp_pm073h.wins, stats_temp_pm073h.losses, stats_temp_pm073h.goal_conceded, stats_temp_pm073h.clean_sheet, stats_temp_pm073h.passes, stats_temp_pm073h.passespermatch, stats_temp_pm073h.passaccuracy, stats_temp_pm073h.crosses, stats_temp_pm073h.crossaccuracy); DROP TABLE public.stats_temp_pm073h; END;", "redshiftTmpDir": "s3://aws-glue-assets-211125327538-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.stats_temp_pm073h", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.stats (team_id INTEGER, season VARCHAR, yellowcards INTEGER, redcards INTEGER, fouls INTEGER, offsides INTEGER, cleansheets INTEGER, goalconceded INTEGER, goalsconcededpermatch DECIMAL, saves INTEGER, tackle INTEGER, tacklesuccess VARCHAR, blockedshots INTEGER, interceptions INTEGER, clearances VARCHAR, headedclearance INTEGER, aerialbattleduelswon VARCHAR, errorsleadingtogoal INTEGER, owngoals INTEGER, goals INTEGER, goalspermatch DECIMAL, shots INTEGER, shotsontarget INTEGER, shootingaccuracy VARCHAR, penaltiesscored INTEGER, bigchancescreated INTEGER, hitwoodwork INTEGER, teamname VARCHAR, matches_played INTEGER, wins INTEGER, losses INTEGER, goal_conceded BIGINT, clean_sheet BIGINT, passes VARCHAR, passespermatch DECIMAL, passaccuracy VARCHAR, crosses INTEGER, crossaccuracy VARCHAR); DROP TABLE IF EXISTS public.stats_temp_pm073h; CREATE TABLE public.stats_temp_pm073h (team_id INTEGER, season VARCHAR, yellowcards INTEGER, redcards INTEGER, fouls INTEGER, offsides INTEGER, cleansheets INTEGER, goalconceded INTEGER, goalsconcededpermatch DECIMAL, saves INTEGER, tackle INTEGER, tacklesuccess VARCHAR, blockedshots INTEGER, interceptions INTEGER, clearances VARCHAR, headedclearance INTEGER, aerialbattleduelswon VARCHAR, errorsleadingtogoal INTEGER, owngoals INTEGER, goals INTEGER, goalspermatch DECIMAL, shots INTEGER, shotsontarget INTEGER, shootingaccuracy VARCHAR, penaltiesscored INTEGER, bigchancescreated INTEGER, hitwoodwork INTEGER, teamname VARCHAR, matches_played INTEGER, wins INTEGER, losses INTEGER, goal_conceded BIGINT, clean_sheet BIGINT, passes VARCHAR, passespermatch DECIMAL, passaccuracy VARCHAR, crosses INTEGER, crossaccuracy VARCHAR);"}, transformation_ctx="AmazonRedshift_node1720109369223")

job.commit()
