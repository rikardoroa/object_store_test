select
    personid,
    npi,
    rulenum
from {{ ref('attributionfunctional') }}