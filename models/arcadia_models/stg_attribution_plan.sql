select
    personid,
    npi,
    startdate,
    enddate,
    sourcepartition
from {{ ref('attributionPlan') }}