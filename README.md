# Parking_ticket_analysis
This project explores the intriguining question of whether or not parking ticket issuing frequency has any relationship with the type of weather. Here, I look at 10 years worth of parking ticket data in Chicago and have a total of over 11 million records. I also have the temperature, precipitation, and weather type for the same time period.
<br><br>

To answer this question, I used a combination of SQL, Spark, and Hadoop to work with the big dataset and query it for the information that I was interested in.
<br><br>

The dataset I used is too large to store in this repo, but can be found on https://www.propublica.org/datastore/. Similarly, the weather data can be found on https://www.climate.gov/maps-data/dataset/past-weather-zip-code-data-table.

<br><br>
I joined these two data sources together by joining on the date, completed the analysis using the above mentioned tools, and finally visualized the findings using Python and Seaborn
