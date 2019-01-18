/*
Database Name:    
    trips    
    
Table Names:    
    lime_may_trips    
    ofo_trips    
    zagster_trips
    
Code finds the following:
    1) Total Number of Rides across companies (Count of unique records)
    2) Total Rides by each company (Count of unique trip records)
    3) Total Bikes by each company (Count of unique Bike IDs)
    
*/


# TOTAL NUMBER OF RIDES == 1929
SELECT count(*) 
FROM
	(SELECT * FROM trips.lime_may_trips UNION ALL SELECT * FROM trips.ofo_trips 
        UNION ALL SELECT * FROM trips.zagster_trips) as merged_table;


# INDIVIDUAL COMPANY RIDES
/*
Company Name, Number of Trip IDs, Number of Unique Bike IDs
*/
select count(*) , count(distinct BIKE_ID)  from trips.lime_may_trips;
select count(*) , count(distinct `Bike ID`) from trips.ofo_trips;
select count(*) , count(distinct `Bike ID`) from trips.zagster_trips;


