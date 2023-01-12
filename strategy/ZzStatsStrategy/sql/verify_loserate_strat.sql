/*
if (k.lose_count/k.count)<=0.3, we think this is a peak (2 or -2)
1	59
2	92
-2	133
-1	104
*/
select 
last_dir, count(last_dir)
from anal_zzitems_D_5
where 
startep >= 1641254400 -- 2022-01-04 00:00:00
and km_groupid in
(
select km_groupid from anal_zzkmstats_D_5 k where 
(k.lose_count/k.count)<=0.3
and k.count >= 20
)
group by last_dir

/*
if (k.lose_count/k.count)>=0.7 we think this is not a peak and continue the trend (-1 or 1)
-1	55
2	21
1	53
-2	23
*/
---
select 
last_dir, count(last_dir)
from anal_zzitems_D_5
where 
startep >= 1641254400 -- 2022-01-04 00:00:00
and km_groupid in
(
select km_groupid from anal_zzkmstats_D_5 k where 
(k.lose_count/k.count)>=0.7
and k.count >= 20
)
group by last_dir