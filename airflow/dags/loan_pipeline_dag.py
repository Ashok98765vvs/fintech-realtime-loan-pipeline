"""
Loan Pipeline DAG
==================
Main Airflow DAG orchestrating the end-to-end mortgage loan data pipeline:

  Extract -> Validate -> Load -> Transform (dbt) -> Test -> Alert

Schedule:  Every 15 minutes (SLA: < 15 min end-to-end latency)
Owner:     Ashok Chowdary
Stack:     Apache Airflow 2.8, Snowflake, dbt Cloud, Great Expectations
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default Arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    'owner': 'ashok.chowdary',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['ashoknaidu98765@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=15),
}

# ---------------------------------------------------------------------------
# Connections & Variables (configured in Airflow UI / Secrets Backend)
# ---------------------------------------------------------------------------
SNOWFLAKE_CONN_ID = 'snowflake_fintech'
DBT_PROJECT_DIR = '/opt/airflow/dbt'
SLACK_CONN_ID = 'slack_data_engineering'
GE_CHECKPOINT = 'loan_portfolio_suite'

# ---------------------------------------------------------------------------
# Slack Notification Helpers
# ---------------------------------------------------------------------------
def _build_slack_message(context, status: str) -> str:
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context.get('task_instance').task_id
    log_url = context.get('task_instance').log_url
    icon = ':white_check_mark:' if status == 'SUCCESS' else ':red_circle:'
    return (
        f"{icon} *{status}* | DAG: `{dag_id}` | Task: `{task_id}`\n"
        f"Run ID: `{run_id}`\n"
        f"<{log_url}|View Logs>"
    )


def _on_failure_callback(context):
    msg = _build_slack_message(context, 'FAILURE')
    SlackWebhookOperator(
        task_id='slack_failure',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=msg,
    ).execute(context)


# ---------------------------------------------------------------------------
# Task Functions
# ---------------------------------------------------------------------------
def _extract_raw_loans(**context):
    """Extract raw loan events from Kafka and stage in S3."""
    from ingestion.loan_kafka_producer import LoanEventGenerator
    import json
    import boto3

    logger.info("Starting loan event extraction from Kafka...")
    generator = LoanEventGenerator(events_per_second=500)

    # Extract a batch of events (15 min window)
    events = []
    for _ in range(10_000):  # ~10K events per 15-min window
        from dataclasses import asdict
        events.append(asdict(generator.generate_event()))

    # Stage to S3 for Snowflake COPY INTO
    s3 = boto3.client('s3')
    batch_key = f"loan_events/{context['ds']}/{context['ts_nodash']}.json"
    s3.put_object(
        Bucket='fintech-loan-landing',
        Key=batch_key,
        Body=json.dumps(events)
    )

    logger.info(f"Staged {len(events)} events to s3://fintech-loan-landing/{batch_key}")
    context['ti'].xcom_push(key='batch_key', value=batch_key)
    context['ti'].xcom_push(key='event_count', value=len(events))
    return batch_key


def _validate_raw_data(**context):
    """Run Great Expectations checkpoint on raw loan data."""
    import great_expectations as ge
    from great_expectations.checkpoint import SimpleCheckpoint

    logger.info("Running Great Expectations checkpoint: loan_portfolio_suite")
    context_ge = ge.get_context()
    result = context_ge.run_checkpoint(
        checkpoint_name=GE_CHECKPOINT,
        run_name=f"airflow_{context['ts_nodash']}"
    )

    if not result['success']:
        failed_expectations = [
            exp for exp in result['results']
            if not exp['success']
        ]
        logger.error(f"Data quality FAILED: {len(failed_expectations)} expectations failed")
        for exp in failed_expectations:
            logger.error(f"  - {exp['expectation_config']['expectation_type']}: {exp}")
        raise ValueError(f"Great Expectations checkpoint failed: {len(failed_expectations)} violations")

    logger.info("Data quality validation PASSED")
    return True


def _check_row_count(**context):
    """Short-circuit operator: skip downstream if no new data."""
    event_count = context['ti'].xcom_pull(key='event_count', task_ids='extract_raw_loans')
    if event_count == 0:
        logger.warning("No new loan events found. Skipping downstream tasks.")
        return False
    logger.info(f"Found {event_count} new loan events. Proceeding with pipeline.")
    return True


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id='loan_realtime_pipeline',
    description='End-to-end real-time mortgage loan data pipeline'
                ' | Snowflake + dbt + Great Expectations',
    schedule_interval='*/15 * * * *',   # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                  # Prevent overlapping runs
    default_args=DEFAULT_ARGS,
    on_failure_callback=_on_failure_callback,
    tags=['fintech', 'loans', 'realtime', 'snowflake', 'dbt'],
    doc_md="""
    # Loan Real-Time Pipeline

    **Owner**: Ashok Chowdary
    **SLA**: < 15 minutes end-to-end
    **Stack**: Python | Kafka | Snowflake | dbt Cloud | Great Expectations | Monte Carlo

    ## Pipeline Stages
    1. **Extract**: Pull loan events from Kafka, stage to S3
    2. **Row Count Check**: Short-circuit if no new data
    3. **Validate**: Run Great Expectations data quality checks
    4. **Load**: COPY INTO Snowflake raw layer
    5. **Transform (Staging)**: Run dbt staging models
    6. **Transform (Marts)**: Run dbt mart models (incremental)
    7. **Test**: Run dbt tests + custom data quality assertions
    8. **Alert**: Slack notification on success/failure
    """
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Extract raw loan events from Kafka to S3
    # ------------------------------------------------------------------
    extract_raw_loans = PythonOperator(
        task_id='extract_raw_loans',
        python_callable=_extract_raw_loans,
        doc_md="Extract loan events from Kafka producer and stage in S3."
    )

    # ------------------------------------------------------------------
    # Task 2: Row count check (short-circuit)
    # ------------------------------------------------------------------
    check_row_count = ShortCircuitOperator(
        task_id='check_row_count',
        python_callable=_check_row_count,
        doc_md="Skip downstream if no new events extracted."
    )

    # ------------------------------------------------------------------
    # Task 3: Validate raw data with Great Expectations
    # ------------------------------------------------------------------
    validate_raw_data = PythonOperator(
        task_id='validate_raw_data',
        python_callable=_validate_raw_data,
        doc_md="Run GE checkpoint on raw loan data before loading."
    )

    # ------------------------------------------------------------------
    # Task 4: Load events into Snowflake RAW layer
    # ------------------------------------------------------------------
    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            COPY INTO RAW.LOAN_EVENTS
            FROM @RAW.LOAN_EVENTS_STAGE
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE;
        """,
        doc_md="COPY INTO Snowflake raw table from S3 stage."
    )

    # ------------------------------------------------------------------
    # Task 5: Run dbt staging models
    # ------------------------------------------------------------------
    run_dbt_staging = BashOperator(
        task_id='run_dbt_staging',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run
                --select tag:staging
                --target prod
                --vars '{{"execution_date": "{{{{ ds }}}}"}}'  &&
            echo "dbt staging completed successfully"
        """,
        doc_md="Run dbt staging layer models (views)."
    )

    # ------------------------------------------------------------------
    # Task 6: Run dbt mart models (incremental)
    # ------------------------------------------------------------------
    run_dbt_marts = BashOperator(
        task_id='run_dbt_marts',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run
                --select tag:marts
                --target prod
                --vars '{{"execution_date": "{{{{ ds }}}}"}}'  &&
            echo "dbt marts completed successfully"
        """,
        doc_md="Run dbt mart layer models (incremental tables)."
    )

    # ------------------------------------------------------------------
    # Task 7: Run dbt tests
    # ------------------------------------------------------------------
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt test
                --select tag:critical
                --target prod  &&
            echo "dbt tests PASSED"
        """,
        doc_md="Run dbt data quality tests on critical models."
    )

    # ------------------------------------------------------------------
    # Task 8: Slack success notification
    # ------------------------------------------------------------------
    notify_slack_success = SlackWebhookOperator(
        task_id='notify_slack_success',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":white_check_mark: *Loan Pipeline SUCCESS* | "
            "Run: `{{ run_id }}` | Date: `{{ ds }}` | "
            "Events processed: `{{ ti.xcom_pull(key='event_count', "
            "task_ids='extract_raw_loans') }}`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Send Slack success notification to #data-engineering channel."
    )

    # ------------------------------------------------------------------
    # Task 9: Slack failure notification
    # ------------------------------------------------------------------
    notify_slack_failure = SlackWebhookOperator(
        task_id='notify_slack_failure',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":red_circle: *Loan Pipeline FAILED* | "
            "Run: `{{ run_id }}` | Date: `{{ ds }}` | "
            "Check Airflow logs for details."
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
        doc_md="Send Slack failure notification to #data-engineering channel."
    )

    # ------------------------------------------------------------------
    # DAG Dependency Chain
    # ------------------------------------------------------------------
    (
        extract_raw_loans
        >> check_row_count
        >> validate_raw_data
        >> load_to_snowflake
        >> run_dbt_staging
        >> run_dbt_marts
        >> run_dbt_tests
        >> [notify_slack_success, notify_slack_failure]
    )
