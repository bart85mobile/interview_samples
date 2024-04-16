import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, to_date, to_timestamp, max, is_null, when_matched, when_not_matched, lead
from snowflake.snowpark.window import Window

def model(dbt, session):
    dbt.config(
        schema="staging"
        ,materialized="table"
        ,unique_key=['id', 'date_start', 'date_end']
    )

    rawTableName = dbt.source('stakeholder', 'raw_sales_agent')

    agent_df = (
        rawTableName
        .select(
            col('"post_Code"').as_("post_Code")
            ,col('"area"').as_("area")
            ,col('"source"').as_("source")
            ,col('"status"').as_("status")
            ,col('"street"').as_("street")
            ,col('"sName"').as_("sName")
            ,col('"id"').as_("id")
            ,col('"disId"').as_("disId")
            ,col('"externalId"').as_("externalId")
            ,col('"humanCode"').as_("humanCode")
            ,col('"city"').as_("city")
            ,col('"country"').as_("country")
            ,col('"taxId"').as_("taxId")
            ,col('"sourceId"').as_("sourceId")
            ,col('"kafka_topic"').as_("kafka_topic")
            ,col('"k_partition"').as_("k_partition")
            ,col('"kafka_offset"').as_("kafka_offset")
            ,to_timestamp(col('"k_timestamp"')).as_("k_timestamp")
            ,col('"email"').as_("email")
            ,col('"landlineNum"').as_("landlineNum")
            ,col('"licenseNum"').as_("licenseNum")
            ,col('"mobileNum"').as_("mobileNum")
            ,col('"name"').as_("name")
            ,col('"number"').as_("number")
            ,to_date(col('"k_timestamp"')).as_("date_start")
        )
    )

    filter_agent_df = (
        agent_df.group_by(
            "id"
            ,"k_partition"
            ,to_date(col("k_timestamp"))
        ).agg(
            max("k_timestamp").alias("k_timestamp")
        ).select(
            "id"
            ,"k_partition"
            ,"k_timestamp"
        )
    )

    filtered_agent_df = (
        agent_df.join(
            filter_agent_df
            ,(agent_df.col("id") == filter_agent_df.col("id")) 
                & (agent_df.col("k_partition") == filter_agent_df.col("k_partition"))
                & (agent_df.col("k_timestamp") == filter_agent_df.col("k_timestamp"))
            ,lsuffix=""
            ,rsuffix="_r"
            ,how = "inner"
        )
    ).select(agent_df.col("*"))
    
    window = Window.order_by("date_start")
    final_df = filtered_agent_df.withColumn('date_end', lead(col("date_start")).over(window))
    
    return final_df
