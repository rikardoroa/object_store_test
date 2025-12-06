select
    personid,
    npi,
    sourcepartition
from {{ ref('attributionClinical') }}