select 
    count(distinct other_properties['device.esn']) as num_device_esn_error
    ----input.visitor_device_id
    from dynecom_execution_events
    where other_properties['output.error_code'] = 'account_password_not_set'
    and dateint between 20190201 and 20190228
