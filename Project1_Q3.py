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


mod_rain = sqlContext.sql("select * from main_table where PRCP >= 0.1 and PRCP < 0.3 and SNOW = 0.0")
count_mr = sqlContext.sql("select count(distinct DATE) from main_table where PRCP >= 0.1 and PRCP < 0.3 and SNOW = 0.0")
mr_tick_per_day = (mod_rain.count()/count_mr.collect()[0][0])
mr_tick_per_day = [mr_tick_per_day]
df = sqlContext.createDataFrame((mr_tick_per_day,), ["Total tickets during mod rain"])
mr_total = df.rdd
mr_total.saveAsTextFile('Project1_Total_ticket_during_mod_rain')
mr_prob = (float(mod_rain.count())/float(total_count.count()))
mr_prob = [mr_prob]
df = sqlContext.createDataFrame((mr_prob,), ["Prob of getting ticket during mod rain"])
mr = df.rdd
mr.saveAsTextFile('Project1_Prob_of_getting_ticket_during_mod_rain')


heavy_rain = sqlContext.sql("select * from main_table where PRCP > 0.29 and SNOW = 0.0")
count_hr = sqlContext.sql("select count(distinct DATE) from main_table where PRCP > 0.29 and SNOW = 0.0")
hr_tick_per_day = (heavy_rain.count()/count_hr.collect()[0][0])
hr_tick_per_day = [hr_tick_per_day]
df = sqlContext.createDataFrame((hr_tick_per_day,), ["Total tickets during heavy rain"])
hr_total = df.rdd
hr_total.saveAsTextFile('Project1_Total_ticket_during_heavy_rain')
hr_prob = (float(heavy_rain.count())/float(total_count.count()))
hr_prob = [hr_prob]
df = sqlContext.createDataFrame((hr_prob,), ["Prob of getting ticket during heavy rain"])
hr = df.rdd
hr.saveAsTextFile('Project1_Prob_of_getting_ticket_during_heavy_rain')


no_snow = sqlContext.sql("select * from main_table where SNOW = 0.0")
count_ns = sqlContext.sql("select count(distinct DATE) from main_table where SNOW = 0.0")
ns_tick_per_day = (no_snow.count()/count_ns.collect()[0][0])
ns_tick_per_day = [ns_tick_per_day]
df = sqlContext.createDataFrame((ns_tick_per_day,), ["Total tickets during no snow"])
ns_total = df.rdd
ns_total.saveAsTextFile('Project1_Total_ticket_during_no_snow')
ns_prob = (float(no_snow.count())/float(total_count.count()))
ns_prob = [ns_prob]
df = sqlContext.createDataFrame((ns_prob,), ["Prob of getting ticket during no snow"])
ns = df.rdd
ns.saveAsTextFile('Project1_Prob_of_getting_ticket_during_no_snow')


mod_snow = sqlContext.sql("select * from main_table where SNOW > 0.0 and SNOW < 2.1")
count_ms = sqlContext.sql("select count(distinct DATE) from main_table where SNOW > 0.0 and SNOW < 2.1")
ms_tick_per_day = (mod_snow.count()/count_ms.collect()[0][0])
ms_tick_per_day = [ms_tick_per_day]
df = sqlContext.createDataFrame((ms_tick_per_day,), ["Total tickets during mod snow"])
ms_total = df.rdd
ms_total.saveAsTextFile('Project1_Total_ticket_during_mod_snow')
ms_prob = (float(mod_snow.count())/float(total_count.count()))
ms_prob = [ms_prob]
df = sqlContext.createDataFrame((ms_prob,), ["Prob of getting ticket during mod snow"])
ms = df.rdd
ms.saveAsTextFile('Project1_Prob_of_getting_ticket_during_mod_snow')


heavy_snow = sqlContext.sql("select * from main_table where SNOW > 2.0")
count_hs = sqlContext.sql("select count(distinct DATE) from main_table where SNOW > 2.0")
hs_tick_per_day = (heavy_snow.count()/count_hs.collect()[0][0])
hs_tick_per_day = [hs_tick_per_day]
df = sqlContext.createDataFrame((hs_tick_per_day,), ["Total tickets during heavy snow"])
hs_total = df.rdd
hs_total.saveAsTextFile('Project1_Total_ticket_during_heavy_snow')
hs_prob = (float(heavy_snow.count())/float(total_count.count()))
hs_prob = [hs_prob]
df = sqlContext.createDataFrame((hs_prob,), ["Prob of getting ticket during heavy snow"])
hs = df.rdd
hs.saveAsTextFile('Project1_Prob_of_getting_ticket_during_heavy_snow')


low_temp = sqlContext.sql("select * from main_table where TAVG <= 32")
count_lt = sqlContext.sql("select count(distinct DATE) from main_table where TAVG <= 32")
lt_tick_per_day = (low_temp.count()/count_lt.collect()[0][0])
lt_tick_per_day = [lt_tick_per_day]
df = sqlContext.createDataFrame((lt_tick_per_day,), ["Total tickets during low temp"])
lt_total = df.rdd
lt_total.saveAsTextFile('Project1_Total_ticket_during_low_temp')
lt_prob = (float(low_temp.count())/float(total_count.count()))
lt_prob = [lt_prob]
df = sqlContext.createDataFrame((lt_prob,), ["Prob of getting ticket during low temp"])
lt = df.rdd
lt.saveAsTextFile('Project1_Prob_of_getting_ticket_during_low_temp')


mid1_temp = sqlContext.sql("select * from main_table where TAVG > 32 and TAVG <=50")
count_m1 = sqlContext.sql("select count(distinct DATE) from main_table where TAVG > 32 and TAVG <=50")
m1_tick_per_day = (mid1_temp.count()/count_m1.collect()[0][0])
m1_tick_per_day = [m1_tick_per_day]
df = sqlContext.createDataFrame((m1_tick_per_day,), ["Total tickets during mid1 temp"])
m1_total = df.rdd
m1_total.saveAsTextFile('Project1_Total_ticket_during_mid1_temp')
m1_prob = (float(mid1_temp.count())/float(total_count.count()))
m1_prob = [m1_prob]
df = sqlContext.createDataFrame((m1_prob,), ["Prob of getting ticket during mid1 temp"])
m1 = df.rdd
m1.saveAsTextFile('Project1_Prob_of_getting_ticket_during_mid1_temp')


mid2_temp = sqlContext.sql("select * from main_table where TAVG >=51 and TAVG <=69")
count_m2 = sqlContext.sql("select count(distinct DATE) from main_table where TAVG >=51 and TAVG <=69")
m2_tick_per_day = (mid2_temp.count()/count_m2.collect()[0][0])
m2_tick_per_day = [m2_tick_per_day]
df = sqlContext.createDataFrame((m2_tick_per_day,), ["Total tickets during mid2 temp"])
m2_total = df.rdd
m2_total.saveAsTextFile('Project1_Total_ticket_during_mid2_temp')
m2_prob = (float(mid2_temp.count())/float(total_count.count()))
m2_prob = [m2_prob]
df = sqlContext.createDataFrame((m2_prob,), ["Prob of getting ticket during mid2 temp"])
m2 = df.rdd
m2.saveAsTextFile('Project1_Prob_of_getting_ticket_during_mid2_temp')


high_temp = sqlContext.sql("select * from main_table where TAVG >= 70")
count_ht = sqlContext.sql("select count(distinct DATE) from main_table where TAVG >= 70")
ht_tick_per_day = (high_temp.count()/count_ht.collect()[0][0])
ht_tick_per_day = [ht_tick_per_day]
df = sqlContext.createDataFrame((ht_tick_per_day,), ["Total tickets during high temp"])
ht_total = df.rdd
ht_total.saveAsTextFile('Project1_Total_ticket_during_high_temp')
ht_prob = (float(high_temp.count())/float(total_count.count()))
ht_prob = [ht_prob]
df = sqlContext.createDataFrame((ht_prob,), ["Prob of getting ticket during high temp"])
ht = df.rdd
ht.saveAsTextFile('Project1_Prob_of_getting_ticket_during_high_temp')
