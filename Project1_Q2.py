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

q4 = sqlContext.sql("select violation_description, PRCP from main_table")
q5 = sqlContext.sql("select violation_description, SNOW from main_table")
q6 = sqlContext.sql("select violation_description, TAVG from main_table")

rdd = q4.rdd.map(tuple)
rdd = rdd.mapValues(lambda x: float(x))
avg = (rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: float(x[0])/x[1]))
avg.collect()
avg = avg.sortBy(lambda x: x[1], False)
avg.saveAsTextFile("Project1AvgPRCPPerTicketType")
