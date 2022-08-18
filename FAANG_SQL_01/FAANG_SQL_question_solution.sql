create table warehouse
(
	ID						varchar(10),
	OnHandQuantity			int,
	OnHandQuantityDelta		int,
	event_type				varchar(10),
	event_datetime			timestamp
);

insert into warehouse values
('SH0013', 278,   99 ,   'OutBound', '2020-05-25 0:25'), 
('SH0012', 377,   31 ,   'InBound',  '2020-05-24 22:00'),
('SH0011', 346,   1  ,   'OutBound', '2020-05-24 15:01'),
('SH0010', 346,   1  ,   'OutBound', '2020-05-23 5:00'),
('SH009',  348,   102,   'InBound',  '2020-04-25 18:00'),
('SH008',  246,   43 ,   'InBound',  '2020-04-25 2:00'),
('SH007',  203,   2  ,   'OutBound', '2020-02-25 9:00'),
('SH006',  205,   129,   'OutBound', '2020-02-18 7:00'),
('SH005',  334,   1  ,   'OutBound', '2020-02-18 8:00'),
('SH004',  335,   27 ,   'OutBound', '2020-01-29 5:00'),
('SH003',  362,   120,   'InBound',  '2019-12-31 2:00'),
('SH002',  242,   8  ,   'OutBound', '2019-05-22 0:50'),
('SH001',  250,   250,   'InBound',  '2019-05-20 0:45');
COMMIT;

select * from warehouse order by event_datetime desc;


with wh as 
		(select * from warehouse order by event_datetime desc),
    days as
		(select OnHandQuantity,event_datetime,
		(event_datetime - interval 90 DAY) as day90,
		(event_datetime - interval 180 DAY) as day180,
		(event_datetime - interval 270 DAY) as day270,
		(event_datetime - interval 365 DAY) as day365
		from WH limit 1),
    inv_90_days as
		(select coalesce(sum(OnHandQuantityDelta),0) as DaysOld_90
		from WH cross join days d
		where event_type='InBound'
		and WH.event_datetime >= d.day90),
    inv_90_days_final as
		(select 
			case when DaysOld_90 > d.OnHandQuantity then d.OnHandQuantity
				 else DaysOld_90
			end DaysOld_90
        from inv_90_days
        cross join days d),
	inv_180_days as
		(select coalesce(sum(OnHandQuantityDelta),0) as DaysOld_180
		from WH cross join days d
		where event_type='InBound'
		and WH.event_datetime between d.day180 and d.day90),
	inv_180_days_final as
		(select 
			case when DaysOld_180 > (d.OnHandQuantity - DaysOld_90) then (d.OnHandQuantity - DaysOld_90)
				 else DaysOld_180
			end DaysOld_180
        from inv_180_days
        cross join days d
        cross join inv_90_days_final),
	inv_270_days as
		(select coalesce(sum(OnHandQuantityDelta),0) as DaysOld_270
		from WH cross join days d
		where event_type='InBound'
		and WH.event_datetime between d.day270 and d.day180),
	inv_270_days_final as
		(select 
			case when DaysOld_270 > (d.OnHandQuantity - (DaysOld_90+DaysOld_180)) then (d.OnHandQuantity - (DaysOld_90+DaysOld_180))
				 else DaysOld_270
			end DaysOld_270
        from inv_270_days
        cross join days d
        cross join inv_90_days_final
        cross join inv_180_days_final),
	inv_365_days as
		(select coalesce(sum(OnHandQuantityDelta),0) as DaysOld_365
		from WH cross join days d
		where event_type='InBound'
		and WH.event_datetime between d.day365 and d.day270),
	inv_365_days_final as
		(select 
			case when DaysOld_365 > (d.OnHandQuantity - (DaysOld_90 + DaysOld_180 + DaysOld_270)) then (d.OnHandQuantity - (DaysOld_90 + DaysOld_180 + DaysOld_270))
				 else DaysOld_365
			end DaysOld_365
        from inv_365_days
        cross join days d
        cross join inv_90_days_final
        cross join inv_180_days_final
        cross join inv_270_days_final)
select DaysOld_90 as "0-90 Days old",
DaysOld_180 as "91-180 Day old",
DaysOld_270 as "181-270 Day old",
DaysOld_365 as "271-365 Day old"
from inv_90_days_final
cross join inv_180_days_final
cross join inv_270_days_final
cross join inv_365_days_final;