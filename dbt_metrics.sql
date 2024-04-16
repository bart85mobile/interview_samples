{{
    config(
        materialized='table'
        ,schema='lead_metric'
        ,alias='leads'
    )
}}

with

source_fct_leads as (

    select * from {{ ref('fact_leads') }}

),

source_dim_personss as (

    select * from {{ ref('dim_persons') }}

),

sources as (

    {% set aggregationLevels = {
                    "kpiName, aggregationLevel, periodType, date, acquisitionId, sourceId, country, nationality, ageGroup, gender, locale":
                    "acquisitionId
                    ,dp.sourceId
                    ,dp.country
                    ,dp.nationality
                    ,dp.ageGroup
                    ,dp.gender
                    ,dp.locale"
        }
    %}

    {% for aggregationLevel_key, aggregationLevel_value in aggregationLevels.items() %}

        {% for periodType in ["week", "month"] %}

            {% set status_types = {
                            "leads_generated":null,
                            "leads_open":"where fl.status = 'OPEN'",
                            "leads_contacted":"where fl.status = 'CONTACTED'",
                            "leads_qualified":"where fl.status = 'QUALIFIED'",
                            "leads_unqualified":"where fl.status = 'UNQUALIFIED'",
                            "unknown":"where fl.status not in ('OPEN','CONTACTED','QUALIFIED','UNQUALIFIED')"
                }
            %}

            {% for status_types_key, status_types_value in status_types.items() %}

                select
                    'count_{{ status_types_key }}' as kpiName
                    ,count(fl.lead_id)::string as kpi_value
                    ,'{{ aggregationLevel_key }}' as aggregationLevel
                    ,'year-{{ periodType }}' as periodType
                    ,date_trunc('{{ periodType }}', fl.kafka_created)::date as date
                    ,{{ aggregationLevel_value }}
                from source_fct_leads as fl
                left join source_dim_personss as dp
                    on fl.person_id=dp.person_id

                {{ status_types_value }}
                    
                group by
                    '{{ aggregationLevel_key }}'
                    ,'year-{{ periodType }}'
                    ,date_trunc('{{ periodType }}', fl.kafka_created)::date
                    ,{{ aggregationLevel_value }}

            {{ "union all" if not loop.last }}

            {% endfor %}

        {{ "union all" if not loop.last }}

        {% endfor %}

    {{ "union all" if not loop.last }}

    {% endfor %}

)

select * from sources
