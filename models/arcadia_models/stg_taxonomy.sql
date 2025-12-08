select
    taxonomycode,
    taxonomygroup,
    taxonomyclassification,
    taxonomyspecialization,
    taxonomydescription,
    taxonomynotes,
    createtimestamp,
    updatetimestamp
from {{ ref('taxonomy') }}