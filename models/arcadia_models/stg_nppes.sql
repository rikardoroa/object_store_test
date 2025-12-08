select
    npi,
    description,
    providerprimarytaxonomycode,
    entitytype,
    providerpracticelocationaddress1,
    providerpracticelocationaddress2,
    providerpracticelocationcity,
    providerpracticelocationstate,
    providerpracticelocationzip,
    providerpracticephone,
    providerorgname,
    providerlastname,
    providerfirstname,
    createtimestamp,
    updatetimestamp
from {{ ref('nppes') }}