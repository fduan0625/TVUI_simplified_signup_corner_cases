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
)

select 
input_flow
,count(distinct a.vdid) as num_vdid_attempt_login
,count(distinct (case when error_code is not null then a.vdid end)) as num_vdid_login_error
,count(distinct (case when error_code = 'account_password_not_set' then a.vdid end)) as num_vdid_pw_not_set_error
,count(distinct(case when dn.restricted_do_not_copy['input.visitor_device_id'] is not null then a.vdid end)) as num_vdid_reset_pw_msg_sent
,count(distinct(case when dn2.restricted_do_not_copy['input.visitor_device_id'] is not null then a.vdid end)) as num_vdid_login
,count(distinct(case when dn2.restricted_do_not_copy['input.visitor_device_id'] is not null and dn.restricted_do_not_copy['input.visitor_device_id'] is not null then a.vdid end)) as num_vdid_reset_pw_login

from fduan.PI_password_not_set_20190323  a
left join dynecom_execution_events dn
	 on a.vdid = dn.restricted_do_not_copy['input.visitor_device_id']
	 and dn.dateint between 20190323 and 20190324
	 and (dn.other_properties['output.mode'] = 'CONFIRMPASSWORDRESETEMAILED'  OR
	 	  dn.other_properties['output.mode'] = 'CONFIRMPASSWORDRESETCALLED' OR
	 	  dn.other_properties['output.mode'] = 'CONFIRMPASSWORDRESETTEXTED'
         )


left join dynecom_execution_events dn2
	 on a.vdid = dn2.restricted_do_not_copy['input.visitor_device_id']
	 and dn2.dateint between 20190323 and 20190324
	 and dn2.other_properties['output.visitor_state']='CURRENT_MEMBER'
group by 1;

