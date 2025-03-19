
-- https://anupamchand.medium.com/metadata-driven-framework-for-delta-live-tables-887c1995d4bd

CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.control_table (
    id INT                              COMMENT 'Primary Key: Unique identifier for the record',
    layer_type STRING                   COMMENT 'Indicates if this config is for bronze, silver, or gold',
    json_source_config VARIANT          COMMENT 'Source configs in JSON',
    json_target_config VARIANT          COMMENT 'Target configs in JSON',
    yaml_path STRING                    COMMENT 'Path to YAML or other config file if applicable',
    ts_record_start TIMESTAMP           COMMENT 'Start timestamp of the record validity',
    ts_record_end TIMESTAMP             COMMENT 'End timestamp of the record validity',
    CONSTRAINT pk PRIMARY KEY (id)
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);


-- Creating the data quality rule table
CREATE TABLE IF NOT EXISTS metadata.dq_rule (
    id INT COMMENT 'Primary Key: Unique identifier for the rule',
    rule_id INT COMMENT 'ID of the rule being applied',
    rule_type STRING COMMENT 'Type of rule (e.g., completeness, uniqueness)',
    rule STRING COMMENT 'The actual rule definition',
    description STRING COMMENT 'Detailed description of the rule',
    config_file_name STRING COMMENT 'Configuration file associated with the rule',
    record_start_ts TIMESTAMP COMMENT 'Start timestamp of the rule validity',
    record_end_ts TIMESTAMP COMMENT 'End timestamp of the rule validity',
    record_is_active BOOLEAN COMMENT 'Indicates if the rule is active',
    CONSTRAINT dq_rule_pk PRIMARY KEY (id)
) USING DELTA 
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');


-- Creating the data quality rule assignment table
CREATE TABLE IF NOT EXISTS metadata.dq_rule_assignment (
    id INT COMMENT 'Primary Key: Unique identifier for the rule assignment',
    catalog STRING COMMENT 'Table name where the rule is applied',
    schema STRING COMMENT 'Table name where the rule is applied',
    table_name STRING COMMENT 'Table name where the rule is applied',
    column_name STRING COMMENT 'Column name where the rule is applied',
    rule_id INT COMMENT 'Foreign key referencing dq_rule(id)',
    severity STRING COMMENT 'Severity of the rule violation (e.g., warning, error)',
    config_file_name STRING COMMENT 'Configuration file associated with the rule',
    record_start_ts TIMESTAMP COMMENT 'Start timestamp of the rule validity',
    record_end_ts TIMESTAMP COMMENT 'End timestamp of the rule validity',
    record_is_active BOOLEAN COMMENT 'Indicates if the rule assignment is active',
    CONSTRAINT dq_rule_assignment_pk PRIMARY KEY (id),
    CONSTRAINT fk_rule FOREIGN KEY (rule_id) REFERENCES metadata.dq_rule (id)
) USING DELTA 
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');


-- Creating the audit table for Data Quality (DQ) checks
CREATE TABLE IF NOT EXISTS metadata.dq_audit (
    batch_id STRING                     COMMENT 'Master workflow job run ID; unique identifier',
    dq_task_run_id STRING               COMMENT 'Data Quality pipeline run ID',
    schema_name STRING                  OMMENT 'Schema name of the source table under DQ check',
    table_name STRING                   COMMENT 'Name of the table under DQ check',
    target_schema_name STRING           COMMENT 'Schema name of the target table',
    pipeline_status STRING              COMMENT 'Status of the Data Quality pipeline',
    dq_outcome STRING                   COMMENT 'Outcome indicating if any quarantine records were found',
    source_records_count BIGINT         COMMENT 'Record count in the source table',
    target_records_count BIGINT         COMMENT 'Record count in the target table',
    quarantine_records_count BIGINT     COMMENT 'Number of records quarantined',    
    dq_task_start_timestamp TIMESTAMP   COMMENT 'Start timestamp of the Data Quality check',
    dq_task_end_timestamp TIMESTAMP     COMMENT 'End timestamp of the Data Quality check',
    CONSTRAINT dq_audit_pk PRIMARY KEY (batch_id, dq_task_run_id)
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Creating the audit table for ETL job executions
CREATE TABLE IF NOT EXISTS metadata.etl_job_audit (
    job_id STRING COMMENT 'Job ID of the workflow job',
    job_name STRING COMMENT 'Name of the job',
    job_run_id STRING COMMENT 'Workflow job run ID',
    layer STRING COMMENT 'Data layer (e.g., bronze, silver, gold)',
    job_status STRING COMMENT 'Status of the table load pipeline',
    task_start_timestamp TIMESTAMP COMMENT 'Start timestamp of the task',
    task_end_timestamp TIMESTAMP COMMENT 'End timestamp of the task',
    CONSTRAINT etl_job_audit_pk PRIMARY KEY (job_id, job_run_id)
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);



json_source_config variant
-- Bronze template: 
-- {
--     'source_system'='lakehouse', 
--     'source_file_type'='delta'
--     'source_file_path'='abfss:///container/folder'
-- }

-- Silver template: 
-- {
--     'source_catalog_name'='lab', 
--     'source_schema_name'='bronze'
--     'source_table_name'='transactions'
--     'load_type'='append/merge'
-- }

-- Gold template: 
-- {
--      
--     'file_path'='../../.py/.sql/.ipynb'
-- }

json_target_config variant
-- { 
--      target_catalog string
--      target_schema STRING COMMENT 'Schema name of the gold table',
--      target_table STRING COMMENT 'Table name in the gold layer',
--      target_layer
-- }




