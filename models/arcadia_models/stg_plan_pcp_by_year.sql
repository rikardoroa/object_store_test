select
   year,
   personid,
   npi
from {{ ref('PlanPCPByYear') }}