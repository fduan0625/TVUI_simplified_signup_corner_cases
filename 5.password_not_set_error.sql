create table fduan.PI_password_not_set_20190323 as
	(
select other_properties['output.error_code'] as error_code
      ,other_properties['input.flow'] as input_flow
	  ,restricted_do_not_copy['input.visitor_device_id'] as vdid
	  ,restricted_do_not_copy['device.esn'] as esn

from dynecom_execution_events
where dateint=20190323
and other_properties['input.step'] = 'SIGNIN'
and other_properties['input.visitor_state']='NON_REGISTERED_MEMBER'
);

select 
input_flow
,count(distinct a.vdid) as num_vdid_attempt_login
,count(distinct (case when error_code is not null then a.vdid end)) as num_vdid_login_error
,count(distinct (case when error_code = 'account_password_not_set' then a.vdid end)) as num_vdid_pw_not_set_error
,count(distinct(case when error_code = 'account_password_not_set' and dn2.restricted_do_not_copy['input.visitor_device_id'] is not null then a.vdid end)) as num_vdid_reset_pw_login
,count(distinct(case when dn2.restricted_do_not_copy['input.visitor_device_id'] is not null then a.vdid end)) as num_vdid_login
,count(distinct(case when dn2.restricted_do_not_copy['input.visitor_device_id'] is not null and error_code is not null then a.vdid end)) as num_vdid_login_error_login

from fduan.PI_password_not_set_20190323  a

left join dynecom_execution_events dn2
	 on a.vdid = dn2.restricted_do_not_copy['input.visitor_device_id']
	 and dn2.dateint between 20190323 and 20190324
	 and dn2.other_properties['output.visitor_state']='CURRENT_MEMBER'
group by 1;
		

select 
case when su.acct_id is not null then 1 else 0 end as signup_t_f
,case when setpw.acct_id is not null then 1 else 0 end as setup_pw_t_f
,setpw.member_status
,count(distinct reg.acct_id) as num_distinct_register
,count(distinct su.acct_id) as num_signup
,count(distinct setpw.acct_id) as num_setup_pw

from

	(
	select other_properties['row.values.account_id'] as acct_id
	,other_properties['row.values.membership_status'] as member_status
	,event_utc_ms
	,dateint

	from subscriber2_update_logs 
	where other_properties['operation_name']='CreateAccount'
	and restricted_do_not_copy['row.values.password_status']='EMPTY'
	and dateint between 20190401 and 20190407
	) reg
left join 
	(
	select other_properties['row.values.account_id'] as acct_id
	,other_properties['row.values.membership_status'] as member_status
	,event_utc_ms
	,dateint

	from subscriber2_update_logs 
	where other_properties['operation_name']='StartMembership'
	-- and restricted_do_not_copy['row.values.password_status']='EMPTY'
	and dateint >=  20190401 
	) su
	on reg.acct_id = su.acct_id
	   and reg.dateint <= su.dateint

left join
	(
	select other_properties['row.values.account_id'] as acct_id
	,other_properties['row.values.membership_status'] as member_status
	,event_utc_ms
	,dateint

	from subscriber2_update_logs 
	where other_properties['operation_name']='SetPassword'
	and restricted_do_not_copy['row.before_values.password_status']='EMPTY'
	and restricted_do_not_copy['row.values.password_status']='VALID'
	and dateint >=  20190401
	) setpw

	on reg.acct_id = setpw.acct_id
	   and reg.dateint <= setpw.dateint

group by 1,2,3;		
		
