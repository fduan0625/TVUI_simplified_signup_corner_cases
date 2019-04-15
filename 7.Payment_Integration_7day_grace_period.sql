drop table fduan.PI_abuse_rejoin_ptr;
---reduce to payment integration only
create table fduan.PI_abuse_rejoin_ptr as
	(select ptr.account_id
		-- ,pai.billing_partner_handle as pai
		,ptr.subscrn_id
		,ptr.country_desc
		,ptr.mop_method_name
		,ptr.signup_date
		,ptr.partner_name
		,ptr.is_rejoin
		,ptr.is_free_trial_at_signup
		,fraud.fraud_flag
		,ptr.p1_possible_complete_cnt
		,ptr.p1_invol_cancel_cnt
		,ptr.p1_vol_cancel_cnt
	from dse.ptr_subscrn_signup_retention_up_sum ptr
	left join (select distinct account_id, subscrn_id, fraud_flag
		       from dse.pmt_fraud_acct_flag_d
		       -- where fraud_flag='true'
		       where signup_utc_date >= 20180801)fraud    
			on ptr.account_id = fraud.account_id
			and ptr.subscrn_id = fraud.subscrn_id
 where ptr.signup_date >= 20180801
	and ptr.mop_type<>'Netflix'
	and ptr.is_rev_share_partner = 1
	and ptr.mop_method_name in ('Integrated Payment') -- 'DCB (Partner)','DCB (Netflix)'
		);
    
 /**** frequencey of 7 day invol churn ****/

drop table fduan.PI_abuse_rejoin_ptr_freq;
create table fduan.PI_abuse_rejoin_ptr_freq as
	(
	select current.*
	,before.signup_date as before_signup_date
	,nf_datediff(before.signup_date,current.signup_date) as days_since_last_signup
	,d.billing_end_date 
	,nf_datediff(current.signup_date,d.billing_end_date) as current_sub_days_in_service
	from
	(select a.*
		,ROW_NUMBER() OVER (partition by account_id order by signup_date) as num_subs_seen_before

		from fduan.PI_abuse_rejoin_ptr a
		) current
	left join
		(select a.*
		,ROW_NUMBER() OVER (partition by account_id order by signup_date) as num_subs_seen_before

		from fduan.PI_abuse_rejoin_ptr a
		)before
	on current.account_id = before.account_id
	and current.partner_name = before.partner_name
	and current.num_subs_seen_before = before.num_subs_seen_before+1
	left join dse.billing_subscrn_d d
    	on current.account_id = d.account_id
    	and current.subscrn_id = d.subscrn_id
	);
  
 

    
    
    
