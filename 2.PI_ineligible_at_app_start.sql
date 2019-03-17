select partner_name
,count(distinct input_visitor_device_id) as total_app_start
,count(distinct (case when moptrue2=1 or moptrue1=1 then input_visitor_device_id end)) as total_elible
from
(
select appstart.*
,case when pse.restricted_do_not_copy['RESPONSE_JSON'] like '%"MOP":"true"%' then 1 else 0 end as moptrue1
,case when pse.restricted_do_not_copy['RESPONSE_JSON'] like '%MOP=true%' then 1 else 0 end as moptrue2
from

(
select
partner_name
,input_visitor_device_id
-- ,output_visitor_device_id
-- ,input_step
-- ,case when input_step = 'NONE' then 1 else 0 end as app_start_t_f
,request_id

from dse.dynecom_execution_event_f
where partner_is_int_pay = 'true' --integrated payment
and partner_entitlement_id is null -- not bundle
and partner_offer_id is null -- not PPP
and partner_promotion_id is null -- not PPP
and utc_date = 20190309
and input_flow='tenfootSignUp'
and partner_name not in ('ORANGE_POL','ORANGE', 'TALKTALK','VODAFONE','TELECOMITALIA','STARHUB' ---legacy client-driven partners
						 ,'FETCH_TV','SONY','ROKU','LG','LG_PH','REFERENCE','VESTEL')   -- CE devices
and input_step = 'NONE'
and partner_name in ("COMCAST_COX","TOTALPLAY","COMCAST_SHAW","SKY_GERMANY","KPN","SFR","VIRGIN_MEDIA","BYTEL_TV","BT_INTPAY","TELSTRAINT","SWISSCOM","TDC","LG_UPLUS","TELEFONICA_SPAIN","ALTICE_HOT","VODAFONE_DE","PROXIMUS","FREE_ILIAD","ORANGE_POLAND")
group by 1,2,3) appstart

left join default.account_tracking_events pse
	on appstart.request_id = pse.other_properties['loggingcontext.requestId']
where pse.dateint = 20190309
)a

group by 1;
