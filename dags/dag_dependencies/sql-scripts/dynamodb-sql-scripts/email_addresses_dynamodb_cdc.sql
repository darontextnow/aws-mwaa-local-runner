MERGE INTO {{ params.env }}.core.emailaddresses USING (
    SELECT 
        PARSE_JSON("payload.record_json"): dynamodb : Keys : EmailAddress : S :: VARCHAR(16777216) as email_address,
        PARSE_JSON("payload.record_json"): dynamodb : NewImage : UpdatedAt : N :: TIMESTAMP AS updated_at,
        PARSE_JSON("payload.record_json"): dynamodb : NewImage : ValidationDetails : S :: VARCHAR(16777216)
            AS validation_details,
        PARSE_JSON("payload.record_json"): dynamodb : NewImage : ValidationStatus : S :: VARCHAR(16777216)
            AS validation_status,
        PARSE_JSON("payload.record_json"): eventName :: VARCHAR(16777216) AS event_name,
        PARSE_JSON("payload.record_json"):dynamodb:ApproximateCreationDateTime::TIMESTAMP
            AS approximate_creation_date_time,
        inserted_timestamp,
        s3_file_path,
        date_utc
    FROM prod.party_planner_realtime.streamrecord
    WHERE (inserted_timestamp > '{{ prev_data_interval_end_success or data_interval_start }}')
        AND (inserted_timestamp <= '{{ data_interval_end }}')
        AND (UPPER(instance_id) LIKE '%%EMAIL%%')
    --comment:get the recent event for that emailaddress
    QUALIFY ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY  approximate_creation_date_time DESC) = 1
) AS ppr_source ON (EmailAddresses.email_address = ppr_source.email_address)
WHEN MATCHED
    AND (UPPER(ppr_source.event_name) IN ('MODIFY','REMOVE','INSERT'))
    AND (ppr_source.approximate_creation_date_time > emailaddresses.event_creation_timestamp)
THEN UPDATE SET
    updated_at = ppr_source.updated_at,
    validation_details = ppr_source.validation_details,
    validation_status = ppr_source.validation_status,
    event_name = ppr_source.event_name,
    event_creation_timestamp = ppr_source.approximate_creation_date_time,
    updated_timestamp = CURRENT_TIMESTAMP(),
    s3_file_path = ppr_source.s3_file_path,
    date_utc =  ppr_source.date_utc
WHEN NOT MATCHED THEN INSERT (
    email_address, updated_at, validation_details, validation_status,
    event_name, event_creation_timestamp, updated_timestamp, inserted_timestamp, s3_file_path, date_utc
)
VALUES
(
    ppr_source.email_address,
    ppr_source.updated_at,
    ppr_source.validation_details,
    ppr_source.validation_status,
    ppr_source.event_name,
    ppr_source.approximate_creation_date_time,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    ppr_source.s3_file_path,
    ppr_source.date_utc
);