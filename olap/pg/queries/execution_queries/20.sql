select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'lemon%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1998-10-10'
					and l_shipdate < date '1998-10-10' + interval '1 year'
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'EGYPT'
order by
	s_name;
