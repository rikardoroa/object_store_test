select
    personid,
    npi,
    visitdate,
    medicalind,
    sourcepartition,
    locationname,
    enctype  
from {{ ref('attributionRendering') }}