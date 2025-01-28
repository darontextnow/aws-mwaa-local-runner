"""Dag retrieves PayPal transactions data from braintree API and loads it into Snowflake"""
from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX, DAG_DEFAULT_CATCHUP
from de_utils.constants import SF_CONN_ID, SF_BUCKET
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 7, 18),
    schedule="30 0 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def braintree_extract():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ data_interval_start.date() }}}}/transactions"
    cols = [
        "created_at", "updated_at", "id", '"type"', "status", "amount", "currency",
        "merchant_account_id", "authorization_expires_at", "email", "first_name", "last_name",
        "paypal_payer_status", "paypal_transaction_fee", "paypal_transaction_fee_currency",
        "paypal_authorization_id", "paypal_capture_id", "paypal_payment_id", "cvv_response_code",
        "avs_postal_code_response_code", "avs_street_address_response_code",
        "processor_response_code", "processor_response_text", "processor_response_type",
        "processor_settlement_response_code", "processor_settlement_response_text",
        "settlment_batch_id", "refunded_transaction_id", "paypal_refund_id",
        "gateway_rejection_reason", "status_history"
    ]

    @task
    def collect_paypal_transactions(s3_key: str, **context):
        from de_utils.aws import S3Path
        import json
        import io
        from itertools import chain

        gateway = get_braintree_gateway()
        new_transactions = get_new_transactions(
            gateway=gateway,
            start_date=str(context["data_interval_start"].date()),
            end_date=str(context["data_interval_start"].date() + timedelta(days=1))
        )
        pending_transactions = get_pending_transactions(gateway)
        transactions = process_transactions(transactions=chain(pending_transactions, new_transactions))
        str_transactions = [json.dumps(t) for t in transactions]
        fileobj = io.BytesIO("\n".join(str_transactions).encode())
        S3Path(f"s3://{SF_BUCKET}/{s3_key}").upload_fileobj(fileobj)

    snowflake_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_paypal_snowflake",
        table="braintree_transactions",
        schema="billing_service",
        s3_loc=s3_key,
        file_format="(TYPE = JSON TIME_FORMAT = AUTO)",
        copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
        merge_update_columns=cols,
        merge_insert_columns=cols
    )

    collect_paypal_transactions(s3_key=s3_key) >> snowflake_load


def get_braintree_gateway():
    import braintree
    from airflow.models import Variable
    return braintree.BraintreeGateway(braintree.Configuration(access_token=Variable.get("braintree_token")))


def get_pending_transactions(gateway) -> list:
    from de_utils.tndbo import get_dbo
    dbo = get_dbo(SF_CONN_ID)
    sql = """
        SELECT DISTINCT id
        FROM prod.billing_service.braintree_transactions
        WHERE (status IN ('Authorized', 'Authorizing', 'SettlementPending', 'Settling', 'SubmittedForSettlement'))
    """
    for row in dbo.execute(sql):
        transaction_id = row["id"]
        transaction = gateway.transaction.find(transaction_id)
        yield transaction


def get_new_transactions(
        gateway,
        start_date: str,
        end_date: str
) -> list:
    import braintree
    search_conditions = [
        braintree.TransactionSearch.payment_instrument_type == braintree.PaymentInstrumentType.PayPalAccount,
        braintree.TransactionSearch.created_at.between(start_date, end_date)
    ]
    result_objs = gateway.transaction.search(search_conditions)
    return result_objs.items


def clean_transaction(transaction):
    from decimal import Decimal
    class_fields = {
        "Transaction": [
            "additional_processor_response", "amount", "authorization_expires_at", "avs_error_response_code",
            "avs_postal_code_response_code", "avs_street_address_response_code", "billing_details", "channel",
            "created_at", "currency_iso_code", "custom_fields", "cvv_response_code", "discount_amount",
            "gateway_rejection_reason", "id", "merchant_account_id", "network_transaction_id", "order_id",
            "paypal_details", "plan_id", "processor_authorization_code", "processor_response_code",
            "processor_response_text", "processor_response_type", "processor_settlement_response_code",
            "processor_settlement_response_text", "purchase_order_number", "recurring", "refunded_transaction_id",
            "service_fee_amount", "settlement_batch_id", "shipping_amount", "ships_from_postal_code", "status",
            "status_history", "subscription_id", "tax_amount", "tax_exempt", "type", "updated_at"
        ],
        "PayPalAccount": [
            "authorization_id", "capture_id", "custom_field", "payer_email", "payer_first_name", "payer_id",
            "payer_last_name", "payer_status", "payment_id", "refund_id", "token", "transaction_fee_amount",
            "transaction_fee_currency_iso_code"
        ],
        "Dispute": [
            "amount_disputed", "amount_won", "case_number", "created_at", "currency_iso_code", "evidence",
            "id", "kind", "merchant_account_id", "original_dispute_id", "processor_comments", "reason",
            "reason_code", "reason_description", "received_date", "reference_number", "reply_by_date",
            "status", "status_history", "transaction", "updated_at"
        ],
        "Address": [
            "company", "country_code_alpha2", "country_code_alpha3", "country_code_numeric", "country_name",
            "created_at", "customer_id", "extended_address", "first_name", "id", "last_name", "locality",
            "postal_code", "region", "street_address", "updated_at"
        ],
        "StatusEvent": [
            "timestamp", "status", "amount", "user", "transaction_source"
        ]
    }

    if type(transaction).__name__ in class_fields:
        fields = class_fields[type(transaction).__name__]
        data = {field: clean_transaction(getattr(transaction, field, None)) for field in fields
                if getattr(transaction, field, None)}
        return data
    elif isinstance(transaction, list):
        return [clean_transaction(item) for item in transaction]
    elif isinstance(transaction, Decimal):
        return float(transaction)
    elif isinstance(transaction, datetime):
        return transaction.isoformat("T")
    else:
        return transaction


def transform_transaction(transaction: dict) -> dict:
    from copy import deepcopy
    r = deepcopy(transaction)
    r["currency"] = r.get("currency_iso_code")
    paypal_details = r.get("paypal_details", {})
    r["email"] = paypal_details.get("payer_email")
    r["first_name"] = paypal_details.get("payer_first_name")
    r["last_name"] = paypal_details.get("payer_last_name")
    r["paypal_payer_status"] = paypal_details.get("payer_status")
    r["paypal_transaction_fee"] = paypal_details.get("transaction_fee_amount")
    r["paypal_transaction_fee_currency"] = paypal_details.get("transaction_fee_currency_iso_code")
    r["paypal_authorization_id"] = paypal_details.get("authorization_id")
    r["paypal_capture_id"] = paypal_details.get("capture_id")
    r["paypal_payment_id"] = paypal_details.get("payment_id")
    r["paypal_refund_id"] = paypal_details.get("refund_id")
    return {k: v for k, v in r.items() if v}


def process_transactions(transactions) -> list[dict]:
    """Returns cleaned and transformed list of dict objects from given braintree Transactions"""
    processed_trans = {}
    for t in transactions:
        if t.id not in processed_trans:
            new_tran = clean_transaction(t)
            new_tran = transform_transaction(new_tran)
            processed_trans[t.id] = new_tran
    return list(processed_trans.values())


braintree_extract()

if __name__ == "__main__":
    braintree_extract().test(execution_date="2023-07-17")
