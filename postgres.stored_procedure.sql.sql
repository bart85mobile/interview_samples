CREATE OR REPLACE PROCEDURE _stg.sp_round(
    p_setup_list text,
    p_setup text,
    p_time_option char(1),
    p_start_date date,
    p_end_date date)
LANGUAGE 'plpgsql'
    
AS $BODY$
DECLARE
    rec_partition record;
    rec record;
    v_msg text;
    v_context text;
BEGIN
/*---------------------------------------------------------------------------------------------------------------------------------------
REFRESH TABLE
---------------------------------------------------------------------------------------------------------------------------------------*/
/****************************************************************************************************************************************
Name:
    _stg.sp_round

Description:
    Incrementally Exctract Round's Data, Stored Procedure Handles the Reload of Previously Loaded Rows with Delete and Insert.
	
Populate:
    _stg.round

Sources:
    stg_******.round
****************************************************************************************************************************************/
/*---------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS
---------------------------------------------------------------------------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS _stg.round
(
    id bytea null,
    type varchar(30) null,
    bonus boolean null,
    round integer null,
    total_credit numeric null,
    lines smallint null,
    debit numeric null,
    session_id bytea null,
    created_at timestamp null,
    setup smallint null,
    date date null
) PARTITION BY RANGE (date);
    
ALTER TABLE _stg.round ADD COLUMN IF NOT EXISTS valid boolean not null DEFAULT True;
    
--CREATE UNIQUE INDEX IF NOT EXISTS _stg_round_id_date ON _stg.round (id, date) INCLUDE (setup, created_at);
    
FOR rec IN
    SELECT
        '_stg' AS schema_name,
        'round' AS table_name,
        TO_CHAR(DATE_TRUNC('day', dd)::date, 'yyyymmdd')::text AS yyyymmdd,
        (DATE_TRUNC('day', dd)::date) AS start_date,
        (DATE_TRUNC('day', dd) + interval '1 day')::date AS end_date
    FROM GENERATE_SERIES(p_start_date::timestamp, (NOW() + interval '3 day')::timestamp, '1 day'::interval) AS dd
LOOP
    EXECUTE format(E'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (''%s'') TO (''%s'')',
        (SELECT (rec.schema_name)::text),
        (SELECT (rec.table_name || '_' || rec.yyyymmdd)::text),
        (SELECT (rec.schema_name)::text),
        (SELECT (rec.table_name)::text),
        (SELECT (rec.start_date)::text),
        (SELECT (rec.end_date)::text));
END LOOP;
/*---------------------------------------------------------------------------------------------------------------------------------------
Find Last Executable Partitions of Round Table
---------------------------------------------------------------------------------------------------------------------------------------*/
CREATE TABLE tempLast_RoundPartition
(
    parent_schema text not null,
    parent text not null,
    child_schema text not null,
    child text not null,
    partition_extension text not null,
    partition_start date not null,
    partition_end date not null
);
    
CREATE TABLE tempLast_RoundPartition_AllPartitions
(
    is_executable boolean not null,
    parent_schema text not null,
    parent text not null,
    child_schema text not null,
    child text not null,
    partition_extension text not null,
    partition_start date not null,
    partition_end date not null
);
    
IF 'a' = p_time_option THEN
    INSERT INTO tempLast_RoundPartition_AllPartitions
    SELECT
        CASE
            WHEN NOW()::date > (TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') - interval '1 day')::date
                AND NOW()::date < (TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') + interval '3 day')::date THEN True
            ELSE False
        END AS is_executable,
        nmsp_parent.nspname::text AS parent_schema,
        parent.relname::text AS parent,
        nmsp_child.nspname::text AS child_schema,
        child.relname::text AS child,
        '_' || RIGHT(child.relname, 8) AS partition_extension,
        TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') AS partition_start,
        (TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') + interval '1 day')::date AS partition_end
    FROM pg_inherits
    INNER JOIN pg_class parent
        ON pg_inherits.inhparent = parent.oid
    INNER JOIN pg_class child 
        ON pg_inherits.inhrelid = child.oid
    INNER JOIN pg_namespace nmsp_parent
        ON nmsp_parent.oid = parent.relnamespace
    INNER JOIN pg_namespace nmsp_child
        ON nmsp_child.oid = child.relnamespace
    WHERE nmsp_parent.nspname = '_stg' AND parent.relname = 'round';
    
    INSERT INTO tempLast_RoundPartition
    SELECT
        p.parent_schema, p.parent, p.child_schema, p.child, p.partition_extension, p.partition_start, p.partition_end
    FROM
    (
        SELECT
            is_executable, parent_schema, parent, child_schema, child, partition_extension, partition_start, partition_end
        FROM tempLast_RoundPartition_AllPartitions
    ) AS p
    WHERE p.is_executable = True;
ELSE
    INSERT INTO tempLast_RoundPartition_AllPartitions
    SELECT
        CASE
            WHEN p_end_date::date > (TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') - interval '1 day')::date
                AND p_start_date::date < (TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') + interval '3 day')::date THEN True
            ELSE False
        END AS is_executable,
        nmsp_parent.nspname::text AS parent_schema,
        parent.relname::text AS parent,
        nmsp_child.nspname::text AS child_schema,
        child.relname::text AS child,
        '_' || RIGHT(child.relname, 8) AS partition_extension,
        TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') AS partition_start,
        (TO_DATE(RIGHT(child.relname, 8)::text, 'YYYYMMDD') + interval '1 day')::date AS partition_end
    FROM pg_inherits
    INNER JOIN pg_class parent
        ON pg_inherits.inhparent = parent.oid
    INNER JOIN pg_class child 
        ON pg_inherits.inhrelid = child.oid
    INNER JOIN pg_namespace nmsp_parent
        ON nmsp_parent.oid = parent.relnamespace
    INNER JOIN pg_namespace nmsp_child
        ON nmsp_child.oid = child.relnamespace
    WHERE nmsp_parent.nspname = '_stg' AND parent.relname = 'round';
    
    INSERT INTO tempLast_RoundPartition
    SELECT
        p.parent_schema, p.parent, p.child_schema, p.child, p.partition_extension, p.partition_start, p.partition_end
    FROM
    (
        SELECT
            is_executable, parent_schema, parent, child_schema, child, partition_extension, partition_start, partition_end
        FROM tempLast_RoundPartition_AllPartitions
    ) AS p
    WHERE p.is_executable = True;
END IF;
    
DROP TABLE tempLast_RoundPartition_AllPartitions;
    
CREATE TABLE tempLast_RoundPerPartition
(
    partition_extension text not null,
    partition_start date not null,
    partition_end date not null,
    setup_list text not null,
    setup smallint not null,
    start_created_at timestamp not null,
    end_created_at timestamp not null
);
    
FOR rec_partition IN
    SELECT
        partition_extension,
        partition_start,
        partition_end
    FROM tempLast_RoundPartition
LOOP
    /*-----------------------------------------------------------------------------------------------------------------------------------
    Find Last Round Per Setup
    -----------------------------------------------------------------------------------------------------------------------------------*/
    FOR rec IN 
        SELECT
            TRIM(UNNEST(string_to_array(p_setup_list, ','))) AS setup_list,
            TRIM(UNNEST(string_to_array(p_setup, ','))) AS setup,
            p_start_date AS start_created_at
    LOOP
        IF 'a' = p_time_option THEN
            EXECUTE format(E'INSERT INTO tempLast_RoundPerPartition
            VALUES (%L, %L, %L, %L, %L,
            (
                SELECT
                    COALESCE(MAX(created_at) - interval ''5 minute'', ''%s''::timestamp) AS start_created_at
                FROM _stg.round%I
                WHERE setup = %L
            ),
            (
                SELECT
                    NOW()::date::timestamp + interval ''1 day'' - interval ''1 millisecond'' AS end_created_at
            ))',
            (SELECT (rec_partition.partition_extension)::text),
            (SELECT (rec_partition.partition_start)::date),
            (SELECT (rec_partition.partition_end)::date),
            (SELECT (rec.setup_list)::text),
            (SELECT (rec.setup)::smallint),
            (SELECT (rec.start_created_at)::text),
            (SELECT (rec_partition.partition_extension)::text),
            (SELECT (rec.setup)::smallint));
        ELSE
            INSERT INTO tempLast_RoundPerPartition
            VALUES (
            (SELECT (rec_partition.partition_extension)::text),
            (SELECT (rec_partition.partition_start)::date),
            (SELECT (rec_partition.partition_end)::date),
            (SELECT (rec.setup_list)::text),
            (SELECT (rec.setup)::smallint),
            (
                SELECT
                    p_start_date::timestamp AS start_created_at
            ),
            (
                SELECT
                    p_end_date::timestamp + interval '1 day' - interval '1 millisecond' AS end_created_at
            ));
        END IF;
    END LOOP;
END LOOP;
    
CREATE TABLE tempLast_Round
(
    partition_extension text not null,
    partition_start date not null,
    partition_end date not null,
    setup_list text not null,
    setup smallint not null,
    start_created_at timestamp not null,
    end_created_at timestamp not null
);
    
INSERT INTO tempLast_Round
SELECT
    r.partition_extension, r.partition_start, r.partition_end, r.setup_list, r.setup, p.start_created_at, p.end_created_at
FROM tempLast_RoundPerPartition AS r
LEFT JOIN
(
    SELECT 
        setup_list, setup, MAX(start_created_at) AS start_created_at, MAX(end_created_at) AS end_created_at
    FROM tempLast_RoundPerPartition
    GROUP BY setup_list, setup
) AS p
ON r.setup_list = p.setup_list AND r.setup = p.setup
WHERE r.partition_start >= p.start_created_at::date;
    
DROP TABLE tempLast_RoundPerPartition;
/*---------------------------------------------------------------------------------------------------------------------------------------
UPDATE
---------------------------------------------------------------------------------------------------------------------------------------*/
FOR rec IN
    SELECT
        partition_extension,
        partition_start,
        partition_end,
        setup_list,
        setup,
        start_created_at,
        end_created_at
    FROM tempLast_Round
LOOP
    /*-----------------------------------------------------------------------------------------------------------------------------------
    DELETES
    -----------------------------------------------------------------------------------------------------------------------------------*/
    EXECUTE format(E'DELETE
    FROM _stg.round%I
    WHERE setup = %L 
        AND (date >= ''%s'' AND date < ''%s'')
        AND (created_at >= ''%s'' AND created_at <= ''%s'')',
    (SELECT (rec.partition_extension)::text),
    (SELECT (rec.setup)::smallint),
    (SELECT (rec.partition_start)::date),
    (SELECT (rec.partition_end)::date),
    (SELECT (rec.start_created_at)::timestamp),
    (SELECT (rec.end_created_at)::timestamp));
    /*-----------------------------------------------------------------------------------------------------------------------------------
    INSERTS
    -----------------------------------------------------------------------------------------------------------------------------------*/
    EXECUTE format(E'INSERT INTO _stg.round%I
    SELECT 
        id, type, bonus, round, total_bet AS total_credit, lines, win AS debit,
        session_id, created_at, setup, created_at::date AS date
    FROM %I.round
    WHERE (created_at::date >= ''%s'' AND created_at::date < ''%s'')
        AND (created_at >= ''%s'' AND created_at <= ''%s'')',
    (SELECT (rec.partition_extension)::text),
    (SELECT (rec.setup_list)::text),
    (SELECT (rec.partition_start)::date),
    (SELECT (rec.partition_end)::date),
    (SELECT (rec.start_created_at)::timestamp),
    (SELECT (rec.end_created_at)::timestamp));
END LOOP;
    
DROP TABLE tempLast_Round;
DROP TABLE tempLast_RoundPartition;
/*---------------------------------------------------------------------------------------------------------------------------------------
Error Handling
---------------------------------------------------------------------------------------------------------------------------------------*/
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS 
        v_msg = message_text,
        v_context = pg_exception_context;
    
    RAISE 'message: %
    context: %', v_msg, v_context;	
/*---------------------------------------------------------------------------------------------------------------------------------------
EXIT POINT
---------------------------------------------------------------------------------------------------------------------------------------*/
COMMIT;
END; $BODY$;
