
select 
dy.partner_name
-- ,case when mop_category in (12,13,14,15) then 'PI'
--      when mop_category in (-9,-1) then 'unknown'
--      else 'Netflix_MOP' end as mop_cat
,count(distinct visitor_device_id) n_allocs
,count(distinct (case when membership_status = 2 then visitor_device_id end)) n_current_members
,count(distinct (case when membership_status = 2 then visitor_device_id end))*1.0/count(distinct visitor_device_id) as CM_si_rate
,count(distinct visitor_device_id) - count(distinct (case when membership_status = 2 then visitor_device_id end)) n_adjusted
,count(distinct if(signup_utc_ts_ms is not null, visitor_device_id, NULL)) overall_signups
,count(distinct (case when mop_category in (12,13,14,15) and signup_utc_ts_ms is not null then visitor_device_id end)) as PI_signup
,cast(count(distinct if(signup_utc_ts_ms is not null, visitor_device_id, NULL)) as double)/(count(distinct visitor_device_id)  - count(distinct if(membership_status = 2, visitor_device_id, NULL))) signup_rate
from dse.ab_nm_alloc_f ab
inner join 
	(
	select
	distinct
	partner_name
	,input_visitor_device_id
	,utc_date
	-- ,output_visitor_device_id
	-- ,input_step
	-- ,case when input_step = 'NONE' then 1 else 0 end as app_start_t_f
	from dse.dynecom_execution_event_f
	where partner_is_int_pay = 'true' --integrated payment
	and partner_entitlement_id is null -- not bundle
	and partner_offer_id is null -- not PPP
	and partner_promotion_id is null -- not PPP
	and utc_date between 20190201 and 20190228
	and input_flow='tenfootSignUp'
	and partner_name not in ('ORANGE_POL','ORANGE', 'TALKTALK','VODAFONE','TELECOMITALIA','STARHUB' ---legacy client-driven partners
							 ,'FETCH_TV','SONY','ROKU','LG','LG_PH','REFERENCE','VESTEL')   -- CE devices

	)dy
on ab.visitor_device_id=dy.input_visitor_device_id
	and ab.allocation_region_date between nf_dateadd(utc_date,-1) and nf_dateadd(utc_date,+1)
where test_id = 8101 
and allocation_region_date between 20190201 and 20190228
group by 1;
