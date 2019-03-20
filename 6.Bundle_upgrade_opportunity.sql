
-- 2.2 Upgrade opportunity Concurrent Streaming


with bundle_activations as (
select account_id,subscrn_id, signup_plan_id, signup_billing_partner_desc, signup_date
-- 3088  --2S
-- 4001  --1S
-- 3108  --4S
from dse.subscrn_d
where
    signup_date >= 20181001
    and signup_billing_partner_desc in ('TMOBILE_US_BILLED','COMCAST_BILLED'
  ,'SKY_UK_IE_BILLED','FREE_ILIAD_BILLED','TELEFONICA_SPAIN_BILLED'
  ,'SKY_GERMANY_BILLED','KDDI_BILLED')
    and coalesce(fraud_flag, 0) = 0
    and is_tester = 0
),

accts_exceed_limit as (
select distinct customer_id
from vault.conc_stream_block_f
where country_iso_code in ('US','FR','JP','DE','GB','ES')
and event_utc_date >=20181001
),

accts_upgrade_plan as (
select account_id,subscrn_id,from_plan_rollup_id, to_plan_rollup_id
from dse.subscrn_plan_change_f
where country_iso_code in ('US','FR','JP','DE','GB','ES')
and from_plan_rollup_id in (4001,3088)
and to_plan_rollup_id = 3108
and event_date>=20181001
),


accts_limits as (
select bn.signup_plan_id,bn.signup_billing_partner_desc
,nf_datetrunc('month',bn.signup_date) as signup_month
,count(distinct bn.subscrn_id) as num_activations
,count(distinct aa.customer_id) as num_activate_exceed
,count(distinct pp.subscrn_id) as num_upgrade_plan
,count(distinct(case when aa.customer_id is not null then pp.subscrn_id end)) as num_exceed_limit_upgrade_plan
from bundle_activations bn
left join accts_exceed_limit aa
  on bn.account_id = aa.customer_id
left join (select distinct account_id, subscrn_id from accts_upgrade_plan) pp
  on bn.account_id = pp.account_id
  and bn.subscrn_id = pp.subscrn_id
group by 1,2,3
)

select * from accts_limits;

-- 2.3 Upgrade opportunity 4K device


with bundle_activations as (
select account_id,subscrn_id, signup_plan_id, signup_billing_partner_desc,signup_device_type_id, signup_date
-- 3088  --2S
-- 4001  --1S
-- 3108  --4S
from dse.subscrn_d
where
    signup_date >= 20181001
    and signup_billing_partner_desc in ('TMOBILE_US_BILLED','COMCAST_BILLED'
  ,'SKY_UK_IE_BILLED','FREE_ILIAD_BILLED','TELEFONICA_SPAIN_BILLED'
  ,'SKY_GERMANY_BILLED','KDDI_BILLED')
    and coalesce(fraud_flag, 0) = 0
    and is_tester = 0
),

ptr_4k as (
select distinct account_id
from dse.ptr_subscrn_signup_retention_up_sum
where signup_device_type_extended_name in (select distinct device_type_extended_name 
  from dse.device_model_rollup_d
  where model_max_resolution_type='4K')
and  signup_dateint >=20181001
),
accts_upgrade_plan as (
select account_id,subscrn_id,from_plan_rollup_id, to_plan_rollup_id
from dse.subscrn_plan_change_f
where country_iso_code in ('US','FR','JP','DE','GB','ES')
and from_plan_rollup_id in (4001,3088)
and to_plan_rollup_id = 3108
and event_date>=20181001
),

accts_4k as (
select signup_plan_id,signup_billing_partner_desc
,nf_datetrunc('month',signup_date) as signup_month
,count(distinct bn.subscrn_id) as num_activations
,count(distinct (case when aa.account_id is not null then bn.subscrn_id end)) as num_activate_4k
,count(distinct pp.subscrn_id) as num_upgrade_plan
,count(distinct(case when aa.account_id is not null then pp.subscrn_id end)) as num_4k_upgrade_plan

from bundle_activations bn
  left join ptr_4k aa
  on bn.account_id = aa.account_id
left join (select distinct account_id, subscrn_id from accts_upgrade_plan) pp
  on bn.account_id = pp.account_id
  and bn.subscrn_id = pp.subscrn_id
group by 1,2,3
)

select * from accts_4k;
