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

modRainTickets = mod_rain.select('violation_description').rdd.map(lambda x: x.violation_description)
findModRain = modRainTickets.map(lambda x: word_re.findall(x))
modRainTicketsJoin = findModRain.map(lambda v : s.join(v))
iterateModRain = modRainTicketsJoin.map(lambda x: (x, 1))
finalModRain = iterateModRain.reduceByKey(lambda x, y: x + y)
finalModRain = finalModRain.sortBy(lambda x: x[1], False)
total_modRain = finalModRain.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalModRain.saveAsTextFile('Project1ModRainTicketCounts')
total_modRain = [total_modRain]
df = sqlContext.createDataFrame((total_modRain,), ["Total Count"])
total_modRain = df.rdd
total_modRain.saveAsTextFile('Project1ModRainTotal')

heavyRainTickets = heavy_rain.select('violation_description').rdd.map(lambda x: x.violation_description)
findHeavyRain = heavyRainTickets.map(lambda x: word_re.findall(x))
heavyRainTicketsJoin = findHeavyRain.map(lambda v : s.join(v))
iterateHeavyRain = heavyRainTicketsJoin.map(lambda x: (x, 1))
finalHeavyRain = iterateHeavyRain.reduceByKey(lambda x, y: x + y)
finalHeavyRain = finalHeavyRain.sortBy(lambda x: x[1], False)
total_heavyRain = finalHeavyRain.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalHeavyRain.saveAsTextFile('Project1HeavyRainTicketCounts')
total_heavyRain = [total_heavyRain]
df = sqlContext.createDataFrame((total_heavyRain,), ["Total Count"])
total_heavyRain = df.rdd
total_heavyRain.saveAsTextFile('Project1HeavyRainTotal')

noSnowTickets = no_snow.select('violation_description').rdd.map(lambda x: x.violation_description)
findNoSnow = noSnowTickets.map(lambda x: word_re.findall(x))
noSnowTicketsJoin = findNoSnow.map(lambda v : s.join(v))
iterateNoSnow = noSnowTicketsJoin.map(lambda x: (x, 1))
finalNoSnow = iterateNoSnow.reduceByKey(lambda x, y: x + y)
finalNoSnow = finalNoSnow.sortBy(lambda x: x[1], False)
total_noSnow = finalNoSnow.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalNoSnow.saveAsTextFile('Project1NoSnowTicketCounts')
total_noSnow = [total_noSnow]
df = sqlContext.createDataFrame((total_noSnow,), ["Total Count"])
total_noSnow = df.rdd
total_noSnow.saveAsTextFile('Project1NoSnowTotal')

modSnowTickets = mod_snow.select('violation_description').rdd.map(lambda x: x.violation_description)
findModSnow = modSnowTickets.map(lambda x: word_re.findall(x))
modSnowTicketsJoin = findModSnow.map(lambda v : s.join(v))
iterateModSnow = modSnowTicketsJoin.map(lambda x: (x, 1))
finalModSnow = iterateModSnow.reduceByKey(lambda x, y: x + y)
finalModSnow = finalModSnow.sortBy(lambda x: x[1], False)
total_modSnow = finalModSnow.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalModSnow.saveAsTextFile('Project1ModSnowTicketCounts')
total_modSnow = [total_modSnow]
df = sqlContext.createDataFrame((total_modSnow,), ["Total Count"])
total_modSnow = df.rdd
total_modSnow.saveAsTextFile('Project1ModSnowTotal')

heavySnowTickets = heavy_snow.select('violation_description').rdd.map(lambda x: x.violation_description)
findHeavySnow = heavySnowTickets.map(lambda x: word_re.findall(x))
heavySnowTicketsJoin = findHeavySnow.map(lambda v : s.join(v))
iterateHeavySnow = heavySnowTicketsJoin.map(lambda x: (x, 1))
finalHeavySnow = iterateHeavySnow.reduceByKey(lambda x, y: x + y)
finalHeavySnow = finalHeavySnow.sortBy(lambda x: x[1], False)
total_heavySnow = finalHeavySnow.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalHeavySnow.saveAsTextFile('Project1HeavySnowTicketCounts')
total_heavySnow = [total_heavySnow]
df = sqlContext.createDataFrame((total_heavySnow,), ["Total Count"])
total_heavySnow = df.rdd
total_heavySnow.saveAsTextFile('Project1HeavySnowTotal')

lowTempTickets = low_temp.select('violation_description').rdd.map(lambda x: x.violation_description)
findLowTemp = lowTempTickets.map(lambda x: word_re.findall(x))
lowTempTicketsJoin = findLowTemp.map(lambda v : s.join(v))
iterateLowTemp = lowTempTicketsJoin.map(lambda x: (x, 1))
finalLowTemp = iterateLowTemp.reduceByKey(lambda x, y: x + y)
finalLowTemp = finalLowTemp.sortBy(lambda x: x[1], False)
total_lowTemp = finalLowTemp.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalLowTemp.saveAsTextFile('Project1LowTempTicketCounts')
total_lowTemp = [total_lowTemp]
df = sqlContext.createDataFrame((total_lowTemp,), ["Total Count"])
total_lowTemp = df.rdd
total_lowTemp.saveAsTextFile('Project1LowTempTotal')

mid1TempTickets = mid1_temp.select('violation_description').rdd.map(lambda x: x.violation_description)
findMid1Temp = mid1TempTickets.map(lambda x: word_re.findall(x))
mid1TempTicketsJoin = findMid1Temp.map(lambda v : s.join(v))
iterateMid1Temp = mid1TempTicketsJoin.map(lambda x: (x, 1))
finalMid1Temp = iterateMid1Temp.reduceByKey(lambda x, y: x + y)
finalMid1Temp = finalMid1Temp.sortBy(lambda x: x[1], False)
total_Mid1Temp = finalMid1Temp.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalMid1Temp.saveAsTextFile('Project1Mid1TempTicketCounts')
total_Mid1Temp = [total_Mid1Temp]
df = sqlContext.createDataFrame((total_Mid1Temp,), ["Total Count"])
total_Mid1Temp = df.rdd
total_Mid1Temp.saveAsTextFile('Project1Mid1TempTotal')

mid2TempTickets = mid2_temp.select('violation_description').rdd.map(lambda x: x.violation_description)
findMid2Temp = mid2TempTickets.map(lambda x: word_re.findall(x))
mid2TempTicketsJoin = findMid2Temp.map(lambda v : s.join(v))
iterateMid2Temp = mid2TempTicketsJoin.map(lambda x: (x, 1))
finalMid2Temp = iterateMid2Temp.reduceByKey(lambda x, y: x + y)
finalMid2Temp = finalMid2Temp.sortBy(lambda x: x[1], False)
total_mid2Temp = finalMid2Temp.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalMid2Temp.saveAsTextFile('Project1Mid2TempTicketCounts')
total_mid2Temp = [total_mid2Temp]
df = sqlContext.createDataFrame((total_mid2Temp,), ["Total Count"])
total_mid2Temp = df.rdd
total_mid2Temp.saveAsTextFile('Project1Mid2TempTotal')

highTempTickets = high_temp.select('violation_description').rdd.map(lambda x: x.violation_description)
findHighTemp = highTempTickets.map(lambda x: word_re.findall(x))
highTempTicketsJoin = findHighTemp.map(lambda v : s.join(v))
iterateHighTemp = highTempTicketsJoin.map(lambda x: (x, 1))
finalHighTemp = iterateHighTemp.reduceByKey(lambda x, y: x + y)
finalHighTemp = finalHighTemp.sortBy(lambda x: x[1], False)
total_highTemp = finalHighTemp.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
finalHighTemp.saveAsTextFile('Project1HighTempTicketCounts')
total_highTemp = [total_highTemp]
df = sqlContext.createDataFrame((total_highTemp,), ["Total Count"])
total_highTemp = df.rdd
total_highTemp.saveAsTextFile('Project1HighTempTotal')
