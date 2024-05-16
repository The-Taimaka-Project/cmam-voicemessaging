select pid, phone, language, site, call_type, phone_type, uuid
from data.calls 
where date = current_date and (morning_endstatus is null or morning_endstatus != 'Completed') and afternoon_at_queued is null and actual_call = true