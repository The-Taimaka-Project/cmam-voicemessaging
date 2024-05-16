--DEPENDS ON: pid, currphone, langpref, site, next_exp_date, phoneconsent, status, status_detail, 
--movement_detail, receivecalls, current_week, phoneowner
--NOTE: currphone, langpref, phoneconsent implementation needs to be checked for accuracy.
--NOTE: site implementation does not deal with transfers, based purely on admission
--NOTE: there is currently no way for caregivers to opt out of messages once a phone number is added

select cr.pid, cr.phone, cr.langpref, split_part(cr.site,'-',1) as site, 'drop' as call_type, cr.phoneowner
from data.current cr
left join data.dict dt on cr.pid = dt.pid
where status = 'active' and (status_detail = 'otp' or (status_detail = 'moving' and movement_detail = 'to_otp')) --check patient is active + in OTP
and cr.b_phoneconsent is not null and cr.b_phoneconsent = true --check patient has consented to calls
and cr.phone is not null --check patient has provided a phone number
and cr.pid in (select pid from data.messaging_dict where receivecalls = true) --check patient was randomized to calls
and nvdate is not null --check patient has a valid next visit date
and ((nvdate = current_date-1) or (nvdate = current_date-8) or (nvdate = current_date-15) or (nvdate = current_date-22))--check patient is slated to come in tomorrow (or tomorrow going back 3 weeks)