select partner_name,
       count(distinct b.output_visitor_device_id) error_n,
       count(distinct a.output_Visitor_device_id) n
from
  (select output_visitor_device_id, partner_name
   from dse.dynecom_execution_event_f
   where utc_date between 20190201 and 20190228
     and partner_is_int_pay = 'true' --integrated payment
      and partner_entitlement_id is null -- not bundle
      and partner_offer_id is null -- not PPP
      and partner_promotion_id is null -- not PPP

      and input_flow='tenfootSignUp'
    --  and input_visitor_state in ('NEVER_MEMBER',
    --                              'FORMER_MEMBER')
    --  and output_visitor_state = 'CURRENT_MEMBER'
     and partner_name not in ('ORANGE_POL','ORANGE', 'TALKTALK','VODAFONE','TELECOMITALIA','STARHUB' ---legacy client-driven partners
             ,'FETCH_TV','SONY','ROKU','LG','LG_PH','REFERENCE','VESTEL')   -- CE devices
     and input_step <> 'NONE'
     ) a
left join
  (select output_visitor_device_id
   from dse.dynecom_execution_event_f
   where output_error_code = 'missing_partner_data_for_signup'
     and utc_date between 20190201 and 20190228
          and partner_is_int_pay = 'true' --integrated payment
      and partner_entitlement_id is null -- not bundle
      and partner_offer_id is null -- not PPP
      and partner_promotion_id is null -- not PPP
     and input_flow = 'tenfootSignUp'
     and partner_name not in ('ORANGE_POL','ORANGE', 'TALKTALK','VODAFONE','TELECOMITALIA','STARHUB' ---legacy client-driven partners
             ,'FETCH_TV','SONY','ROKU','LG','LG_PH','REFERENCE','VESTEL') -- CE devices
                 ) b 
                 on a.output_visitor_device_id = b.output_visitor_device_id
group by 1
order by 2 desc;
