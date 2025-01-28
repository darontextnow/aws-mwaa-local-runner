    MERGE INTO {{ params.env }}.core.blocks USING (
        SELECT
            PARSE_JSON("payload.record_json"): dynamodb : Keys : UserID : S :: VARCHAR(16777216) AS user_id,
            PARSE_JSON("payload.record_json"): dynamodb : Keys : Contact : S :: VARCHAR(16777216) AS contact,
            PARSE_JSON("payload.record_json"): dynamodb : NewImage : Reason : S :: VARCHAR(16777216) AS reason,
            PARSE_JSON("payload.record_json"): dynamodb : NewImage : CreatedAt : N :: TIMESTAMP AS created_at,
            PARSE_JSON("payload.record_json"): dynamodb : NewImage : ExpiresAt : N :: TIMESTAMP AS expires_at,
            PARSE_JSON("payload.record_json"): eventName :: VARCHAR(16777216) AS eventname,
            PARSE_JSON("payload.record_json"):dynamodb:ApproximateCreationDateTime :: TIMESTAMP
              AS approximate_creation_date_time,
            inserted_timestamp,
            s3_file_path
        FROM prod.party_planner_realtime.streamrecord
        WHERE (inserted_timestamp > '{{ prev_data_interval_end_success or data_interval_start }}')
            AND (inserted_timestamp <= '{{ data_interval_end }}')
            AND (UPPER(instance_id) LIKE '%%BLOCKS%%')
        --get the recent event for that user+contact combo
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, contact ORDER BY  approximate_creation_date_time DESC) = 1
    ) AS ppr_source ON (blocks.user_id = ppr_source.user_id) AND (blocks.contact = ppr_source.contact)
    WHEN MATCHED AND
      (UPPER(ppr_source.eventname) IN ('MODIFY','REMOVE','INSERT'))
      AND (ppr_source.approximate_creation_date_time > blocks.event_creation_timestamp)
    THEN UPDATE SET
        --dyanmodb events timestamp condition is needed when back-filling
        --    i.e. when the pk is changed in the future dag instances dont overwrite in the backfill process
        reason = ppr_source.reason,
        created_at = ppr_source.created_at,
        expires_at = ppr_source.expires_at,
        eventname = ppr_source.eventname,
        event_creation_timestamp = ppr_source.approximate_creation_date_time,
        updated_timestamp = CURRENT_TIMESTAMP(),
        s3_file_path = ppr_source.s3_file_path
    WHEN NOT MATCHED THEN INSERT (
        user_id, contact, reason, created_at, expires_at, eventname,
        event_creation_timestamp, updated_timestamp, inserted_timestamp, s3_file_path
    )
    VALUES (
        ppr_source.user_id,
        ppr_source.contact,
        ppr_source.reason,
        ppr_source.created_at,
        ppr_source.expires_at,
        ppr_source.eventname,
        ppr_source.approximate_creation_date_time,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        ppr_source.s3_file_path
    );