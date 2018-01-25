

CREATE  KEYSPACE cloudcap
   WITH REPLICATION = {
      'class' : 'SimpleStrategy', 'replication_factor' : 1 }

USE cloudcap

create table top10airportsfromx ( origin text, dest text, avgdelay double, PRIMARY KEY (origin, avgdelay));
create table top10airlinesfromx ( origin text, uniquecarrier text, avgdelay double, PRIMARY KEY (origin, avgdelay)) ;
create table top10airlinesbyxy ( origin text, dest text, uniquecarrier text, avgdelay double, PRIMARY KEY (origin, dest, avgdelay)) ;
