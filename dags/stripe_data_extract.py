from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from de_utils.constants import SF_CONN_ID, SF_BUCKET
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 11, 12),
    schedule="30 1 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1
)
def stripe_data_extract():
    object_tasks = {}

    for object_name in ["charge", "dispute", "refund"]:
        s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ts }}}}/{object_name}"

        @task(task_id=f"collect_{object_name}s")
        def collect(obj_name: str, s3_key: str, data_interval_start=None, data_interval_end=None):
            # List all newly created objects and fetch pending objects by their ids
            obj_ids = fetch_pending_object_ids(obj_name, data_interval_start, data_interval_end)
            objects = collect_objects(obj_name, data_interval_start, data_interval_end, obj_ids=obj_ids)
            # Convert StripeObjects to dictionaries and transform them
            records = objects_to_records(objects)
            # Save processed records to S3
            save_records_to_s3(records, s3_key)

        snowflake_load = S3ToSnowflakeDeferrableOperator(
            task_id=f"load_{object_name}s_snowflake",
            table=f"stripe_{object_name}s",
            schema="billing_service",
            s3_loc=s3_key,
            use_variant_staging=True,
            file_format="(TYPE = JSON STRIP_NULL_VALUES = TRUE)",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
            transform_sql=get_transform_sql(object_name)
        )

        object_tasks[object_name] = [collect(object_name, s3_key), snowflake_load]

    # must run dispute and refunds before running charges
    object_tasks["dispute"][0] >> object_tasks["dispute"][1] >> object_tasks["charge"][0] >> object_tasks["charge"][1]
    object_tasks["refund"][0] >> object_tasks["refund"][1] >> object_tasks["charge"][0] >> object_tasks["charge"][1]


# Helper functions
def get_transform_sql(obj_name: str):
    transform_sqls = {
        "charge": """
            SELECT DISTINCT
                $1:id::VARCHAR(30) AS id,
                $1:created::TIMESTAMP AS created_at,
                $1:updated_at::TIMESTAMP AS updated_at,
                $1:amount::INTEGER AS amount,
                $1:currency::VARCHAR(3) AS currency,
                $1:status::VARCHAR(16) AS status,
                $1:customer::VARCHAR(30) AS customer_id,
                $1:captured::BOOLEAN AS captured,
                $1:paid::BOOLEAN AS paid,
                $1:refunded::BOOLEAN AS refunded,
                $1:amount_refunded::INTEGER AS amount_refunded,
                $1:failure_code::VARCHAR(32) AS failure_code,
                $1:failure_message::VARCHAR(1024) AS failure_message,
                $1:fraud_details:user_report::VARCHAR(16) AS user_reported_fraud_status,
                $1:fraud_details:stripe_report::VARCHAR(16) AS stripe_reported_fraud_status,
                $1:outcome:network_status::VARCHAR(32) AS outcome_network_status,
                $1:outcome:"type"::VARCHAR(16) AS outcome_type,
                $1:outcome:reason::VARCHAR(64) AS outcome_reason,
                $1:outcome:risk_level::VARCHAR(16) AS risk_level,
                $1:outcome:risk_score::INTEGER AS risk_score,
                $1:source:object::VARCHAR(64) AS source,
                $1:source:id::VARCHAR(30) AS source_id,
                $1:source:"type"::VARCHAR(64) AS source_type,
                $1:source:address_line1_check::VARCHAR(16) AS billing_address_line1_check,
                $1:source:address_zip_check::VARCHAR(16) AS billing_address_zip_check,
                $1:source:cvc_check::VARCHAR(16) AS card_cvc_check,
                $1:source:funding::VARCHAR(16) AS card_funding_type,
                $1:livemode::BOOLEAN AS livemode,
                $1:metadata::VARCHAR(4096) AS metadata,
                $1:classification::VARCHAR(128) AS classification,
                $1:client_type::VARCHAR(32) AS client_type,
                $1:product_id::INTEGER AS product_id,
                $1:device_price::INTEGER AS device_price,
                $1:plan_id::INTEGER AS plan_id,
                $1:plan_stripe_id::VARCHAR(30) AS plan_stripe_id,
                $1:plan_price::INTEGER AS plan_price,
                $1:family_member::VARCHAR(64) AS family_member,
                $1:pin_sku::VARCHAR(32) AS pin_sku,
                $1:pin_amount::INTEGER AS pin_amount,
                $1:promo_code::VARCHAR(64) AS promo_code,
                $1:shipping_price::INTEGER AS shipping_price
            FROM staging
            """,
        "dispute": """
            SELECT DISTINCT
                $1:id::VARCHAR(30) AS id,
                $1:created::TIMESTAMP AS created_at,
                $1:updated_at::TIMESTAMP AS updated_at,
                $1:amount::INTEGER AS amount,
                $1:currency::VARCHAR(3) AS currency,
                $1:status::VARCHAR(32) AS status,
                $1:reason::VARCHAR(32) AS reason,
                $1:charge::VARCHAR(30) AS charge_id,
                $1:evidence_details:due_by::TIMESTAMP AS evidence_due_by,
                $1:evidence_details:has_evidence::BOOLEAN AS has_evidence,
                $1:evidence_details:past_due::BOOLEAN AS evidence_past_due,
                $1:evidence_ddtails:submission_count::INTEGER AS evidence_submission_cnt,
                $1:is_charge_refundable::BOOLEAN AS is_charge_refundable,
                $1:livemode::BOOLEAN AS livemode,
                $1:metadata::VARCHAR(4096) AS metadata
            FROM staging
            """,
        "refund": """
            SELECT DISTINCT
                $1:id::VARCHAR(30) AS id,
                $1:created::TIMESTAMP AS created_at,
                $1:updated_at::TIMESTAMP AS updated_at,
                $1:amount::INTEGER AS amount,
                $1:currency::VARCHAR(3) AS currency,
                $1:status::VARCHAR(16) AS status,
                $1:reason::VARCHAR(32) AS reason,
                $1:charge::VARCHAR(30) AS charge_id,
                $1:balance_transaction::VARCHAR(30) AS balance_txn_id,
                $1:receipt_number::VARCHAR(64) AS receipt_number,
                $1:failure_balance_transaction::VARCHAR(30) AS failure_balance_txn_id,
                $1:failure_reason::VARCHAR(32) AS failure_reason,
                $1:metadata::VARCHAR(4096) AS metadata
            FROM staging
            """
    }
    return transform_sqls[obj_name]


def fetch_pending_object_ids(obj_name: str, start: datetime, end: datetime):
    from de_utils.tndbo import get_dbo
    id_prep_sqls = {
        "charge": f"""
                    SELECT DISTINCT id
                    FROM billing_service.stripe_charges
                    WHERE status = 'pending'
                    UNION
                    SELECT DISTINCT c.id
                    FROM billing_service.stripe_charges c
                    LEFT JOIN billing_service.stripe_disputes d
                      ON c.id = d.charge_id
                    WHERE d.updated_at >= '{start.date()}'
                      AND d.updated_at <  '{end.date()}'
                      AND d.status IN ('warning_closed', 'charge_refunded', 'won', 'lost')
                    UNION
                    SELECT DISTINCT c.id
                    FROM billing_service.stripe_charges c
                    LEFT JOIN billing_service.stripe_refunds r
                      ON c.id = r.charge_id
                    WHERE r.updated_at >= '{start.date() }'
                      AND r.updated_at <  '{end.date() }'
                      AND r.status IN ('succeeded', 'failed')
                    """,
        "dispute": """
                    SELECT id
                    FROM billing_service.stripe_disputes
                    WHERE status NOT IN ('warning_closed', 'charge_refunded', 'won', 'lost')
                    """,
        "refund": """
                    SELECT id
                    FROM billing_service.stripe_refunds
                    WHERE status = 'pending'
                    """
    }
    cursor = get_dbo(SF_CONN_ID).execute(id_prep_sqls[obj_name])
    for row in cursor:
        yield row["id"]


def collect_objects(obj_name: str, start: datetime, end: datetime, obj_ids=None, limit: int = 100):
    import stripe
    from itertools import chain
    from stripe import RequestsClient
    from airflow.models import Variable

    created = {"gte": int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()),
               "lt": int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())}
    stripe.api_key = Variable.get("stripe_api_key")
    stripe.default_http_client = RequestsClient()
    obj_endpoint = getattr(stripe, obj_name.title())
    new_objs = obj_endpoint.list(limit=limit, created=created).auto_paging_iter()
    pending_objs = (obj_endpoint.retrieve(obj_id) for obj_id in obj_ids)
    objects = chain(pending_objs, new_objs)
    return objects


def objects_to_records(objects):
    for obj in objects:
        raw_record = to_dict_recursive(obj)
        record = transform(raw_record)
        yield record


def to_dict_recursive(obj):
    from stripe import StripeObject
    dict_obj = dict(obj)
    for key, val in dict_obj.items():
        if isinstance(val, StripeObject):
            dict_obj[key] = to_dict_recursive(val)
        elif isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
            dict_obj[key] = [to_dict_recursive(v) for v in val]
    return dict_obj


def transform(record: dict, updated_at: datetime = None):
    import json
    r = record.copy()
    meta_extract_map = {
        "charge": [
            ("classification", str),
            ("client_type", str),
            ("product_id", int),
            ("device_price", int),
            ("plan_id", int),
            ("plan_stripe_id", str),
            ("plan_price", int),
            ("family_member", str),
            ("pin_sku", str),
            ("pin_amount", int),
            ("promo_code", str),
            ("shipping_price", int)
        ],
        "dispute": None,
        "refund": None
    }
    fields = meta_extract_map[r["object"]]
    metadata = r.pop("metadata")
    # Split metadata into extracted fields and remaining fields
    extracted, remainder = split_metadata(metadata, fields=fields)
    # Bring extracted metadata into the top level
    r.update(extracted)
    # Json encode the remainder, if there is any, and put into `metadata`
    if remainder:
        r["metadata"] = json.dumps(remainder)
    # Add updated_at into record
    updated_at = updated_at or datetime.utcnow()
    r["updated_at"] = int(updated_at.timestamp())
    return r


def split_metadata(metadata: dict, fields: list = None):
    fields = fields or []
    extract = {}
    remainder = metadata.copy()
    for field, field_type in fields:
        if field in remainder:
            field_val = remainder.pop(field)
            # Add type check to make RS Load safer
            try:
                extract[field] = field_type(field_val)
            except ValueError:
                continue
    return extract, remainder


def save_records_to_s3(records, s3_key: str):
    from de_utils.aws import S3Path
    from tempfile import NamedTemporaryFile
    import json

    with NamedTemporaryFile("w") as f:
        for record in records:
            f.write(f"{json.dumps(record)}\n")
        f.flush()
        S3Path(f"s3://{SF_BUCKET}/{s3_key}").upload_file(f.name)


stripe_data_extract()

if __name__ == "__main__":
    stripe_data_extract().test("2024-09-06")
