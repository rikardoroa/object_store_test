select
    personid,
    npi,
    rulenum
from {{ ref('attributionFunctional') }}