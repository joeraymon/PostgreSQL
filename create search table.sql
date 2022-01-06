/*
Aggregate 5 series of monthly search data for distinct
topics and save to the computer as a csv file.
*/

/* Create the tables */
create table bitcoin (
	mon varchar(10),
	num_searches varchar(50)
);
create table inflation (
	mon varchar(10),
	num_searches varchar(50)
);
create table gold (
	mon varchar(10),
	num_searches varchar(50)
);
create table fed (
	mon varchar(10),
	num_searches varchar(50)
);
create table dollar (
	mon varchar(10),
	num_searches varchar(50)
);

/* Load the data. */
copy bitcoin
from '/private/tmp/Bitcoin.csv'
delimiter ','
csv header;
copy inflation
from '/private/tmp/inflation.csv'
delimiter ','
csv header;
copy gold
from '/private/tmp/Gold.csv'
delimiter ','
csv header;
copy fed
from '/private/tmp/fed.csv'
delimiter ','
csv header;
copy dollar
from '/private/tmp/dollar.csv'
delimiter ','
csv header;

/* Join and save to computer */
copy (
	select d.mon as month, d.num_searches as usd_searches, fed_searches, gold_searches, btc_searches, inf_searches
	from dollar as d
	inner join
	(
		select f.mon, f.num_searches as fed_searches, gold_searches, btc_searches, inf_searches
		from fed as f
		inner join
		(
			select g.mon, g.num_searches as gold_searches, btc_searches, inf_searches
			from gold as g
			inner join 
			(
				select b.mon, b.num_searches as btc_searches, i.num_searches as inf_searches
				from bitcoin as b
				inner join inflation as i
				on b.mon = i.mon) as t1
			on g.mon = t1.mon) as t2
		on f.mon = t2.mon) as t3
	on d.mon = t3.mon)
to '/private/tmp/data2.csv'
delimiter ','
csv header;