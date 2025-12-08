{{
    config(
        materialized = 'table',
        file_format = 'parquet',
        schema = 'gold_excise_wrk_v1',
        tags = ['datamart_export']
    )
}}

---------------------------------------------------------
-- CTE: all provider paths (ACL Providers)
---------------------------------------------------------
with acl_providers AS (
    SELECT DISTINCT
        s.personid,
        s.path AS provider_hash
    FROM (
        SELECT
            f.personid,
            stack(
                4,
                security_path.grouper1value,
                security_path.grouper2value,
                security_path.grouper3value,
                security_path.pathsegment
            ) AS path
        FROM (
            SELECT personid, npi FROM {{ ref('stg_attribution_functional') }}
            UNION
            SELECT personid, npi FROM {{ ref('stg_attributionClinical') }}
            UNION
            SELECT personid, npi FROM {{ ref('stg_attribution_plan') }}
            UNION
            SELECT personid, npi FROM {{ ref('stg_attribution_rendering') }} WHERE medicalind = 1
        ) f
        INNER JOIN {{ ref('ProviderFilterHierarchy') }} security_path
            ON security_path.npi = f.npi
    ) s
    WHERE s.path IS NOT NULL
),

---------------------------------------------------------
-- CTE: aggregated ACL providers array
---------------------------------------------------------
acl_arrays AS (
    SELECT
        personid,
        ARRAY_AGG(provider_hash) AS acl_providers
    FROM acl_providers
    GROUP BY personid
),

---------------------------------------------------------
-- CTE: base person list (customers + ids)
---------------------------------------------------------
persons AS (
    SELECT DISTINCT
        personid,
        'arcgd' AS customer
    FROM acl_providers
),

---------------------------------------------------------
-- CTE: JSON per person
---------------------------------------------------------
json_per_person AS (
    SELECT
        p.personid,
        p.customer,
        named_struct(
            'customer', p.customer,
            'fragment', concat('spr@urn:doid:arcadia.io:person!', p.customer, '.', p.personid),
            'policyID', concat('person_', p.personid),
            'accessControlPolicy',
                named_struct(
                    'personId', cast(p.personid as string),
                    'DeleteInd', 'N',
                    'policyEntries',
                        array(
                            named_struct(
                                'sourceObjectURI', concat('urn:doid:arcadia.io:person!', p.customer, '.', p.personid),
                                'lastUpdated', cast(current_timestamp() as string),
                                'accessControlLists',
                                    named_struct(
                                        'acl_providers', a.acl_providers,
                                        'acl_sources', array('0')
                                    )
                            )
                        )
                )
        ) AS json_obj
    FROM persons p
    LEFT JOIN acl_arrays a
        ON p.personid = a.personid
)

---------------------------------------------------------
-- FINAL OUTPUT (Spark)
---------------------------------------------------------
SELECT
    personid,
    -- OS FIELDS
    'spr' AS os_id,
    concat('urn:doid:arcadia.io:person!', json_obj.customer, '.', personid) AS os_subject,
    'accesspolicyitem' AS os_datatype,
    'Person' AS os_type,
    -- TOP LEVEL SCALARS
    json_obj.customer AS customer,
    json_obj.fragment AS fragment,
    json_obj.policyID  AS policyID,
    -- STRING JSON NO ESCAPADO
    to_json(json_obj.accessControlPolicy) AS accessControlPolicy
FROM json_per_person;