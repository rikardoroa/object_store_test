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
            SELECT personid, npi FROM {{ ref('AttributionFunctional') }}
            UNION
            SELECT personid, npi FROM {{ ref('AttributionClinical') }}
            UNION
            SELECT personid, npi FROM {{ ref('AttributionPlan') }}
            UNION
            SELECT personid, npi FROM {{ ref('AttributionRendering') }} WHERE medicalind = 1
        ) f
        INNER JOIN {{ ref('provider_filter_hierarchy') }} security_path
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
)

---------------------------------------------------------
-- CTE: JSON per person
---------------------------------------------------------
,
json_per_person AS (
    SELECT
        p.personid,
        p.customer,

        OBJECT_CONSTRUCT(
            'customer', p.customer,
            'fragment', 'spr@urn:doid:arcadia.io:person!' || p.customer || '.' || p.personid,
            'policyID', 'person_' || p.personid,
            'accessControlPolicy',
                OBJECT_CONSTRUCT(
                    'personId', TO_VARCHAR(p.personid),
                    'DeleteInd', 'N',
                    'policyEntries',
                        ARRAY_CONSTRUCT(
                            OBJECT_CONSTRUCT(
                                'sourceObjectURI',
                                    'urn:doid:arcadia.io:person!' || p.customer || '.' || p.personid,
                                'lastUpdated',
                                    TO_VARCHAR(CURRENT_TIMESTAMP()),
                                'accessControlLists',
                                    OBJECT_CONSTRUCT(
                                        'acl_providers', a.acl_providers,
                                        'acl_sources', ARRAY_CONSTRUCT('0')
                                    )
                            )
                        )
                )
        ) AS person_json

    FROM persons p
    LEFT JOIN acl_arrays a
        ON p.personid = a.personid
)

---------------------------------------------------------
-- FINAL FLATTENED OUTPUT PER PERSON
---------------------------------------------------------
SELECT
    personid,

    -----------------------------------------------------
    -- OS FIELDS
    -----------------------------------------------------
    'spr' AS os_id,
    'urn:doid:arcadia.io:person!' || person_json:customer::string || '.' || personid AS os_subject,
    'accesspolicyitem' AS os_datatype,
    'Person' AS os_type,

    -----------------------------------------------------
    -- TOP LEVEL SCALARS
    -----------------------------------------------------
    person_json:customer::string AS customer,
    person_json:fragment::string AS fragment,
    person_json:policyID::string AS policyID,

    -----------------------------------------------------
    -- TOP LEVEL OBJECTS/ARRAYS AS STRING JSON (NO \)
    -----------------------------------------------------
    REPLACE(TO_VARCHAR(person_json:accessControlPolicy), '\\', '') AS accessControlPolicy

FROM json_per_person;
