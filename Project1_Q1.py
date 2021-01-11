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

light_rain = sqlContext.sql("select * from main_table where PRCP > 0.0 and PRCP < 0.1 and SNOW = 0.0")
mod_rain = sqlContext.sql("select * from main_table where PRCP >= 0.1 and PRCP < 0.3 and SNOW = 0.0")
heavy_rain = sqlContext.sql("select * from main_table where PRCP > 0.29 and SNOW = 0.0")
no_snow = sqlContext.sql("select * from main_table where SNOW = 0.0")
mod_snow = sqlContext.sql("select * from main_table where SNOW > 0.0 and SNOW < 2.1")
heavy_snow = sqlContext.sql("select * from main_table where SNOW > 2.0")
low_temp = sqlContext.sql("select * from main_table where TAVG <= 32")
mid1_temp = sqlContext.sql("select * from main_table where TAVG > 32 and TAVG <=50")
mid2_temp = sqlContext.sql("select * from main_table where TAVG >=51 and TAVG <=69")
high_temp = sqlContext.sql("select * from main_table where TAVG >= 70")

import re
s = "-"
word_re = re.compile(r"[\w']+")

lightRainTickets = light_rain.select('violation_description').rdd.map(lambda x: x.violation_description)
findLightRain = lightRainTickets.map(lambda x: word_re.findall(x))
lightRainTicketsJoin = findLightRain.map(lambda v : s.join(v))
iterateLightRain = lightRainTicketsJoin.map(lambda x: (x, 1))
finalLightRain = iterateLightRain.reduceByKey(lambda x, y: x + y)
finalLightRain = finalLightRain.sortBy(lambda x: x[1], False)
total_lightRain = finalLightRain.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalLightRain.saveAsTextFile('Project1LightRainTicketTicketCounts')
total_lightRain = [total_lightRain]
df = sqlContext.createDataFrame((total_lightRain,), ["Total Count"])
total_lightRain = df.rdd
total_lightRain.saveAsTextFile('Project1LightRainTotal')

