MERGE INTO {{ params.env }}.core.capabilities USING (
    SELECT
        PARSE_JSON("payload.record_json"): dynamodb: NewImage: PK: S :: VARCHAR(16777216) AS pk,
        PARSE_JSON("payload.record_json"): dynamodb: NewImage: SK: S :: VARCHAR(16777216) AS sk,
        SPLIT_PART(pk,'#',2) AS userid,
        PARSE_JSON("payload.record_json"): dynamodb: NewImage: Bundles: L :: ARRAY AS bundles,
        PARSE_JSON("payload.record_json"): dynamodb: NewImage: CreatedAt: N :: TIMESTAMP AS createdat,
        PARSE_JSON("payload.record_json"): dynamodb: NewImage: UpdatedAt: N :: TIMESTAMP AS updatedat,
        PARSE_JSON("payload.record_json"): dynamodb: NewImage: Version: N :: NUMBER AS version,
        PARSE_JSON("payload.record_json"): eventName :: VARCHAR(16777216) AS eventname,
        PARSE_JSON("payload.record_json"):dynamodb:ApproximateCreationDateTime::TIMESTAMP AS approximate_creation_date_time,
        inserted_timestamp,
        s3_file_path
    FROM prod.party_planner_realtime.streamrecord
    WHERE (inserted_timestamp > '{{ prev_data_interval_end_success or data_interval_start }}')
        AND (inserted_timestamp <= '{{ data_interval_end }}')
        AND (UPPER(instance_id) LIKE '%%CAPABILITIES%%')
    --get the recent event for that primary key pk+sk
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pk, sk ORDER BY approximate_creation_date_time DESC, updatedat DESC) = 1
) AS ppr_source ON (capabilities.pk = ppr_source.pk) AND (capabilities.sk = ppr_source.SK)
WHEN MATCHED
  AND (UPPER(ppr_source.eventname) IN ('MODIFY', 'REMOVE', 'INSERT'))
  AND (ppr_source.approximate_creation_date_time > capabilities.event_creation_timestamp)
THEN UPDATE SET
    --dyanmodb events timestamp condition is needed for the back-filling
    --    usecase ie when the pk is changed in the future dag instances dont overwrite in the backfill process
    sk = ppr_source.sk,
    bundles = ppr_source.bundles,
    createdat = ppr_source.createdat,
    updatedat = ppr_source.updatedat,
    version = ppr_source.version,
    eventname = ppr_source.eventname,
    event_creation_timestamp = ppr_source.approximate_creation_date_time,
    updated_timestamp = CURRENT_TIMESTAMP(),
    s3_file_path = ppr_source.s3_file_path
WHEN NOT matched THEN INSERT (
    pk,
    sk,
    userid,
    bundles,
    createdat,
    updatedat,
    version,
    eventname,
    event_creation_timestamp,
    s3_file_path,
    updated_timestamp,
    inserted_timestamp
)
VALUES (
    ppr_source.pk,
    ppr_source.sk,
    ppr_source.userid,
    ppr_source.bundles,
    ppr_source.createdat,
    ppr_source.updatedat,
    ppr_source.version,
    ppr_source.eventname,
    ppr_source.approximate_creation_date_time,
    ppr_source.s3_file_path,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);