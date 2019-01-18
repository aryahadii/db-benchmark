select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1998-11-20'
	and l_shipdate < date '1998-11-20' + interval '1 year'
	and l_discount between 0.5 - 0.01 and 0.5 + 0.01
	and l_quantity < 2;
