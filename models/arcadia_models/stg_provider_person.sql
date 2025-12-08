select
    -- Metadata Hudi (se conservan por trazabilidad, pero como string)
    _hoodie_commit_time,        
    _hoodie_commit_seqno,       
    _hoodie_record_key,      
    _hoodie_partition_path,  
    _hoodie_file_name,         
    -- Provider core fields
    providerpersonid,
    lastname,
    firstname,
    provfullname,
    npi,
    specialty,
    providerid,
    sourceid,
    activeind,
    deleteind,
    inserttimestamp,
    updatetimestamp
from {{ ref('providerperson') }}