{{
    config(
        materialized = 'table',
        file_format = 'parquet',
        schema = 'gold_excise_wrk_v1',
        tags = ['datamart_export']
    )
}}

---------------------------------------------------------
-- CTE: source
---------------------------------------------------------
WITH source AS (
    SELECT
        COALESCE(m.source_id, p.source_id) AS source_id,
        COALESCE(m.source_name, p.source_name) AS source_name
    FROM (
        SELECT
            source_id,
            MAX(_arcadia_source) AS source_name
        FROM {{ ref('stg_plan_member') }}
        GROUP BY source_id
    ) m
    FULL OUTER JOIN (
        SELECT
            CAST(center_id AS STRING) AS source_id,
            MAX(_arcadia_source) AS source_name
        FROM {{ ref('stg_patient') }}
        GROUP BY center_id
    ) p
    ON m.source_id = p.source_id
),

---------------------------------------------------------
-- CTE: rendering
---------------------------------------------------------
rendering AS (
    SELECT
        personid,
        npi,
        sourcepartition,
        COUNT(DISTINCT visitdate) AS visitcount,
        MIN(visitdate) AS firstvisit,
        MAX(visitdate) AS lastvisit
    FROM {{ ref('stg_attribution_rendering') }}
    WHERE medicalind = 1
    GROUP BY personid, npi, sourcepartition
),

---------------------------------------------------------
-- CTE: unified attribution
---------------------------------------------------------
all_attribution AS (
    SELECT personid, npi, 'Functional' AS attributionType, NULL AS sourcepartition
    FROM {{ ref('stg_attribution_functional') }}

    UNION ALL
    SELECT personid, npi, 'Clinical' AS attributionType, sourcepartition
    FROM {{ ref('stg_attributionClinical') }}

    UNION ALL
    SELECT personid, npi, 'Plan' AS attributionType, sourcepartition
    FROM {{ ref('stg_attribution_plan') }}

    UNION ALL
    SELECT personid, npi, 'Plan' AS attributionType, sourcepartition
    FROM rendering
),

---------------------------------------------------------
-- CTE: flattened rows
---------------------------------------------------------
flat AS (
    SELECT
        f.personid,
        pp.providerpersonid AS providerId,
        nppes.description AS providerName,
        f.npi,
        r.firstvisit AS firstVisitDate,
        r.lastvisit AS lastVisitDate,
        r.visitcount AS visitCount,
        f.attributionType,
        CASE
            WHEN f.attributionType = 'Plan' AND eoy.PersonID IS NULL THEN false
            ELSE true
        END AS focal,

        concat(
            taxonomy.TaxonomyClassification,
            CASE
                WHEN taxonomy.TaxonomySpecialization = '' THEN ''
                ELSE concat(': ', taxonomy.TaxonomySpecialization)
            END
        ) AS specialty,

        array(
            nav_path.Grouper1Name,
            nav_path.Grouper2Name,
            nav_path.Grouper3Name,
            nav_path.Name
        ) AS textPath,

        s.source_name AS sourceName

    FROM all_attribution f
    LEFT JOIN rendering r
        ON f.npi = r.npi AND f.personid = r.personid
    INNER JOIN {{ ref('stg_nppes') }} nppes
        ON f.npi = nppes.npi
    INNER JOIN {{ ref('stg_taxonomy') }} taxonomy
        ON nppes.providerPrimaryTaxonomyCode = taxonomy.taxonomycode
    INNER JOIN {{ ref('stg_provider_person') }} pp
        ON CAST(pp.npi AS STRING) = f.npi
    INNER JOIN {{ ref('ProviderFilterHierarchy') }} nav_path
        ON nav_path.npi = f.npi
    LEFT JOIN source s
        ON s.source_id = f.sourcepartition
    LEFT JOIN {{ ref('stg_plan_pcp_by_year') }} eoy
        ON f.personid = eoy.personid
       AND f.npi = eoy.npi
       AND eoy.Year = year(current_date())
),

---------------------------------------------------------
-- CTE: JSON per person (Spark syntax)
---------------------------------------------------------
json_per_person AS (
    SELECT
        personid,
        named_struct(
            'customer', 'arcgd',
            'careteam',
                collect_list(
                    named_struct(
                        'sortOrder', 1,
                        'providerId', providerId,
                        'providerName', providerName,
                        'lastVisitDate', cast(lastVisitDate as string),
                        'firstVisitDate', cast(firstVisitDate as string),
                        'visitCount', cast(visitCount as string),
                        'attributionType', attributionType,
                        'focal', focal,
                        'npi', npi,
                        'specialty', specialty,
                        'textPath', textPath,
                        'source',
                            named_struct('name', sourceName)
                    )
                ),
            'total', count(*)
        ) AS json_obj

    FROM flat
    GROUP BY personid
)

---------------------------------------------------------
-- FINAL SPARK OUTPUT
---------------------------------------------------------
SELECT
    personid,
    -------------------------
    -- OS metadata
    -------------------------
    concat('urn:doid:arcadia.io:person!', json_obj.customer, '.', personid) AS os_id,
    concat('urn:doid:arcadia.io:person!', json_obj.customer, '.', personid) AS os_subject,
    'careteam' AS os_datatype,
    'Person' AS os_type,
    -------------------------
    -- scalar fields
    -------------------------
    json_obj.customer AS customer,
    json_obj.total    AS total,
    -------------------------
    -- arrays/objects as JSON string
    -------------------------
    to_json(json_obj.careteam) AS careteam
FROM json_per_person;
