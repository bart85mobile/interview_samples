CREATE OR REPLACE PROCEDURE PLAYGROUND.TRANS.P_CALCULATION_INPUTS("P_DATE_OPTION" VARCHAR(1), "P_START_DATE" DATE, "P_END_DATE" DATE)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
declare
    v_start_date date;
    v_end_date date;
begin
    //start - extract data from the source//
    if (p_date_option = ''a'') then
        create or replace temporary table trans.max_calculation_inputs as
        select
            max(last_modified_date)::date as last_modified_date
        from trans.calculation_inputs;

        v_start_date := (select coalesce(last_modified_date,(:p_start_date)) from trans.max_calculation_inputs);
        v_end_date := (current_date()+1);

        drop table trans.max_calculation_inputs;
    else
        v_start_date := p_start_date;
        v_end_date := (p_end_date+1);
    end if;

    create or replace temporary table trans.non_pivoted_calculation_inputs as
    with extracted_data as
    (
        select distinct
            row_number() over (partition by json_string:_id."$oid"::string, i.value:name::string order by json_string:last_modified_date."$date" desc) as row_number,
            json_string:_id."$oid"::string as calculation_oid,
            lower(replace(replace(replace((i.value:name::string),''-'',''__''),''.'',''_''),'' '',''_'')) as inputs_name,
            coalesce(i.value:value::string, ''unknown'') as inputs_value,
            to_timestamp(json_string:created_date."$date"::bigint/1000) as created_date,
            to_timestamp(json_string:last_modified_date."$date"::bigint/1000) as last_modified_date
        from policy_db.source_db.calculations,
        table(flatten(json_string:inputs)) i
        where last_modified_date >= (:v_start_date)::timestamp AND last_modified_date < (:v_end_date)::timestamp
    )
    select
        calculation_oid,inputs_name,inputs_value,created_date,last_modified_date
    from extracted_data
    where row_number=1;

    create or replace temporary table trans.distinct_inputs_name as
    select distinct inputs_name
    from trans.non_pivoted_calculation_inputs;
    //end - extract data from the source//

    //start - pivot non_pivoted_calculation_inputs by inputs_name//
    let v_pivot_query string :=
    (
        select
            ''create or replace temporary table trans.pivoted_calculation_inputs as select *
            from 
            (
                select *
                from trans.non_pivoted_calculation_inputs
            ) as p
            pivot (listagg(inputs_value) for inputs_name in (''||listagg(distinct ''''''''||inputs_name||'''''''','','')||''));''
        from trans.non_pivoted_calculation_inputs
    );
    execute immediate :v_pivot_query;

    drop table trans.non_pivoted_calculation_inputs;
    //end - pivot non_pivoted_calculation_inputs by inputs_name//

    //start - rename columns for pivoted_calculation_inputs//
    let v_rename_columns string:=
    (
        select
            listagg(''case when ''||(''"''''''||inputs_name||''''''"='')||'''''''''' then ''''not applicable'''' else ''||(''"''''''||inputs_name||''''''"'')||'' end as ''||(inputs_name),'','') within group (order by inputs_name)
        from trans.distinct_inputs_name
    );
    let v_rename_columns_table string:=''create or replace temporary table trans.temp_calculation_inputs as
        select
            calculation_oid,
            created_date,
            last_modified_date,
            ''||(:v_rename_columns)||''
        from trans.pivoted_calculation_inputs;'';
    execute immediate (:v_rename_columns_table);

    drop table trans.pivoted_calculation_inputs;
    //end - rename columns for pivoted_calculation_inputs//

    //start - create the calculation_inputs table and add columns to it//
    let v_column_list string:=
    (
        select 
            listagg((inputs_name)||'' string not null default ''''not applicable'''''','','') within group (order by inputs_name)
        from trans.distinct_inputs_name
    );
    let v_create_table string:=''create table if not exists trans.calculation_inputs (
        calculation_oid string not null,
        created_date timestamp not null,
        last_modified_date timestamp not null,
        ''||(:v_column_list)||'',
        constraint pk_calculation_inputs primary key (calculation_oid));'';
    execute immediate (:v_create_table);

    let c1 cursor for 
        select inputs_name
        from trans.distinct_inputs_name
        except
        select lower(column_name) as inputs_name
        from information_schema.columns
        where lower(table_schema) =''trans'' and lower(table_name) =''calculation_inputs'';

    for rec in c1 loop
        let v_new_column string := rec.inputs_name;
        let v_add_column string := ''alter table trans.calculation_inputs add column ''||(:v_new_column)||'' string not null default ''''not applicable'''''';
        execute immediate (:v_add_column);
    end loop;
    close c1;
    //end - create the calculation_inputs table and add columns to it//

    //start - update and insert records//
    let v_update_different string:=
    (
        select
            listagg(''t.''||(inputs_name)||''<>s.''||(inputs_name),'' or '') within group (order by inputs_name)
        from trans.distinct_inputs_name
    );
    let v_update_equal string:=
    (
        select
            listagg(''t.''||(inputs_name)||''=s.''||(inputs_name),'','') within group (order by inputs_name)
        from trans.distinct_inputs_name
    );
    let v_insert_names string:=
    (
        select
            listagg((inputs_name),'','') within group (order by inputs_name)
        from trans.distinct_inputs_name
    );
    let v_insert_values string:=
    (
        select
            listagg(''s.''||(inputs_name),'','') within group (order by inputs_name)
        from trans.distinct_inputs_name
    );
    drop table trans.distinct_inputs_name;

    let v_merge string:=''merge into trans.calculation_inputs as t
    using trans.temp_calculation_inputs as s
    on t.calculation_oid=s.calculation_oid
    when matched and t.last_modified_date < ''''''||(:v_end_date)::timestamp||''''''
    and
    (
        t.created_date<>s.created_date
        or t.last_modified_date<>s.last_modified_date
        or ''||(:v_update_different)||''
    )
    then update
    set 
        t.created_date=s.created_date,
        t.last_modified_date=s.last_modified_date,
        ''||(:v_update_equal)||''
    when not matched
    then insert
    (
        calculation_oid,
        created_date,
        last_modified_date,
        ''||(:v_insert_names)||''
    )
    values
    (
        s.calculation_oid,
        s.created_date,
        s.last_modified_date,
        ''||(:v_insert_values)||''
    );'';
    execute immediate (:v_merge);

    drop table trans.temp_calculation_inputs;
    //end - update and insert records//

    return ''Update from ''||(:v_start_date)||'' to ''||(:v_end_date)||'' Completed'';
end; ';
