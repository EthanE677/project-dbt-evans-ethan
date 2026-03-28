
    
    

select
    ID as unique_field,
    count(*) as n_records

from SNOWBEARAIR_DB.RAW.SFTP_API_EVANS_E
where ID is not null
group by ID
having count(*) > 1


