create table fduan.MVPD_PI_PIN as 
    (

select base_see_pin.*
    ,case when pin_error.in_acct_id is not null then 1 else 0 end as PIN_error_t_f
    ,pin_error.in_acct_id as PIN_error_acct_id
    ,pin_error.pai as PIN_error_pai
from
(select 
    dee.other_properties['partner.name'] as partner_name
    ,dee.dateint
    ,dee.other_properties['input.account_owner_id'] as in_acct_id
    ,dee.other_properties['partner.pai']  as pai
    ,ae.billing_partner_handle as  pai_that_completed_signup
    
from dynecom_execution_events dee
left outer join (
    select
        dateint as dateint,
        other_properties['billing_partner'] as billing_partner,
        other_properties['billing_partner_handle'] as billing_partner_handle
    from account_events
    where dateint between 20190201 and 20190228
    and other_properties['event_type'] = 'CREATE_MEMBERSHIP') ae
on dee.dateint = ae.dateint
and dee.other_properties['partner.name'] = ae.billing_partner
and dee.other_properties['partner.pai'] = ae.billing_partner_handle
where dee.dateint between 20190201 and 20190228
and dee.other_properties['input.flow'] = 'tenfootSignUp'
and dee.other_properties['output.step'] = 'PAYMENTPIN' ---- here to identify PIN page
and dee.other_properties['partner.name'] is not null
and dee.other_properties['partner.name'] not in (
'ORANGE',
'TALKTALK',
'VODAFONE',
'TELECOMITALIA',
'STARHUB'
)

group by 1,2,3,4,5) base_see_pin

left join

(select 
    dee.other_properties['partner.name'] as partner_name
    ,dee.dateint
    ,dee.other_properties['input.account_owner_id'] as in_acct_id
    ,dee.other_properties['partner.pai'] as pai
    
    from dynecom_execution_events dee

    where dee.dateint between 20190201 and 20190228
    and dee.other_properties['input.flow'] = 'tenfootSignUp'
    and dee.other_properties['input.step'] = 'PAYMENTPIN' ---- here to identify PIN page
    and dee.other_properties['partner.name'] is not null
    and dee.other_properties['output.error_code'] in ('Partner_pin_limit_reached','externalPartner_invalidPIN')
    and dee.other_properties['partner.name'] not in (
    'ORANGE',
    'TALKTALK',
    'VODAFONE',
    'TELECOMITALIA',
    'STARHUB'
    )
    group by 1,2,3,4
)pin_error

on base_see_pin.partner_name = pin_error.partner_name
    and base_see_pin.dateint = pin_error.dateint
    and base_see_pin.in_acct_id = pin_error.in_acct_id
    and base_see_pin.pai = pin_error.pai
)

--- all device_type_id 

-- pin page signup
select partner_name
,count(distinct pai) as num_pin_accounts
,count(distinct PIN_error_pai) as num_pin_error
,count(distinct pai_that_completed_signup) as num_pin_signup
from fduan.MVPD_PI_PIN
group by 1;





-- compare against test 8101
select 
device.partner_name
,case when pin.in_acct_id is not null then 1 else 0 end as pin_page_t_f
-- ,case when pin.PIN_error_t_f = 1 then 1 else 0 end as pin_error_t_f
,count(distinct account_id) as n_allocs
-- ,case when mop_category in (12,13,14,15) then 'PI'
--      when mop_category in (-9,-1) then 'unknown'
--      else 'Netflix_MOP' end as mop_cat
-- ,count(distinct visitor_device_id) n_allocs
,count(distinct if(membership_status = 2, account_id, NULL)) n_current_members
,count(distinct account_id) - count(distinct if(membership_status = 2, account_id, NULL)) n_adjusted
,count(distinct if(signup_utc_ts_ms is not null, account_id, NULL)) overall_signups
,count(distinct (case when mop_category in (12,13,14,15) then account_id end)) as n_pi_signup
from dse.ab_nm_alloc_f ab
    inner join (select case when brand='Proximus' then 'Proximus'
       else mso_partner end as partner_name
        ,device_type_id
        from dse.device_type_rollup_d
        where (mso_partner in ('Shaw','Cox','British Telecom','Aussie Broadband','PCCW','Comcast','Altice HOT','KPN')
        or brand = 'Proximus')
        ) device
    on ab.device_type_id = device.device_type_id
left join fduan.MVPD_PI_PIN pin
    on ab.account_id = cast(pin.in_acct_id as BIGINT)

where ab.test_id = 8101 
and ab.allocation_region_date between 20190201 and 20190228
group by 1,2;



-- Compare against subscrn_d
select 
a.partner_name
,count(distinct pai_that_completed_signup) as num_pi_signup
,count(distinct (case when a.pai_that_completed_signup is null then d.subscrn_id end)) as num_nflx_signup

from fduan.MVPD_PI_PIN a
    left join dse.subscrn_d d
    on cast(a.in_acct_id as bigint)= d.account_id
    and d.signup_date between 20190201 and 20190228
group by 1;


