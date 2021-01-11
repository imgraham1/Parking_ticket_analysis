from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext



sc = SparkContext(appName="PySparksi618f19avg_stars_per_category")
sqlContext = SQLContext(sc)

tickets_df = sqlContext.read.format("csv").option("header", "true").load("chicago_tickets.csv")

weather_df = sqlContext.read.format("csv").option("header", "true").load("chicago_weather.csv")


tickets_df.registerTempTable("tickets")
weather_df.registerTempTable("weather")
q1 = sqlContext.sql("select DATE, PRCP, SNOW, TAVG, WSF5 from weather")
q2 = sqlContext.sql("select issue_date, community_area_name, violation_description from tickets where issue_date like '2017%'")
q3 = sqlContext.sql("select weather.DATE, tickets.violation_description, tickets.community_area_name, weather.PRCP, weather.SNOW, weather.TAVG, weather.WSF5 from tickets inner join weather on substring(tickets.issue_date,1,10)=weather.DATE")
q3.registerTempTable("main_table")
total_count = sqlContext.sql("select * from main_table")


light_rain = sqlContext.sql("select * from main_table where PRCP > 0.0 and PRCP < 0.1 and SNOW = 0.0")
count_lr = sqlContext.sql("select count(distinct DATE) from main_table where PRCP > 0.0 and PRCP < 0.1 and SNOW = 0.0")
lr_tick_per_day = (light_rain.count()/count_lr.collect()[0][0])
lr_tick_per_day = [lr_tick_per_day]
df = sqlContext.createDataFrame((lr_tick_per_day,), ["Total tickets during light rain"])
lr_total = df.rdd
lr_total.saveAsTextFile('Project1_Total_ticket_during_light_rain')
lr_prob = (float(light_rain.count())/float(total_count.count()))
lr_prob = [lr_prob]
df = sqlContext.createDataFrame((lr_prob,), ["Prob of getting ticket during light rain"])
lr = df.rdd
lr.saveAsTextFile('Project1_Prob_of_getting_ticket_during_light_rain')


