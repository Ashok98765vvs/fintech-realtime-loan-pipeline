"""
Snowflake Loader for Loan Events
=================================
Loads raw loan events from Kafka into Snowflake raw layer using
the Snowflake Connector for Python. Supports batch and streaming modes.

Author: Ashok Chowdary
Stack: Python 3.11, Snowflake Connector, boto3
"""

import json
import logging
from typing import List, Dict, Any
from datetime import datetime
from dataclasses import dataclass

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    database: str = 'FINTECH_DB'
    schema: str = 'RAW'
    warehouse: str = 'INGEST_WH'
    role: str = 'DATA_ENGINEER'


CREATE_RAW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS RAW.LOAN_EVENTS (
    EVENT_ID        VARCHAR(36)    NOT NULL,
    LOAN_ID         VARCHAR(20)    NOT NULL,
    BORROWER_ID     VARCHAR(20)    NOT NULL,
    TENANT_ID       VARCHAR(50)    NOT NULL,
    EVENT_TYPE      VARCHAR(20)    NOT NULL,
    LOAN_TYPE       VARCHAR(20),
    LOAN_PURPOSE    VARCHAR(20),
    ORIGINATION_DATE DATE,
    MATURITY_DATE    DATE,
    ORIGINAL_BALANCE FLOAT,
    CURRENT_BALANCE  FLOAT,
    INTEREST_RATE    FLOAT,
    MONTHLY_PAYMENT  FLOAT,
    PAYMENT_AMOUNT   FLOAT,
    DAYS_PAST_DUE    INTEGER DEFAULT 0,
    PROPERTY_STATE   VARCHAR(2),
    CREDIT_SCORE     INTEGER,
    LTV_RATIO        FLOAT,
    DTI_RATIO        FLOAT,
    EVENT_TIMESTAMP  TIMESTAMP_NTZ,
    IS_VALID         BOOLEAN DEFAULT TRUE,
    LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (EVENT_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw loan events ingested from Kafka stream';
"""

MERGE_SQL = """
MERGE INTO RAW.LOAN_EVENTS AS target
USING (
    SELECT
        $1:event_id::VARCHAR        AS EVENT_ID,
        $1:loan_id::VARCHAR         AS LOAN_ID,
        $1:borrower_id::VARCHAR     AS BORROWER_ID,
        $1:tenant_id::VARCHAR       AS TENANT_ID,
        $1:event_type::VARCHAR      AS EVENT_TYPE,
        $1:loan_type::VARCHAR       AS LOAN_TYPE,
        $1:loan_purpose::VARCHAR    AS LOAN_PURPOSE,
        $1:origination_date::DATE   AS ORIGINATION_DATE,
        $1:maturity_date::DATE      AS MATURITY_DATE,
        $1:original_balance::FLOAT  AS ORIGINAL_BALANCE,
        $1:current_balance::FLOAT   AS CURRENT_BALANCE,
        $1:interest_rate::FLOAT     AS INTEREST_RATE,
        $1:monthly_payment::FLOAT   AS MONTHLY_PAYMENT,
        $1:payment_amount::FLOAT    AS PAYMENT_AMOUNT,
        $1:days_past_due::INTEGER   AS DAYS_PAST_DUE,
        $1:property_state::VARCHAR  AS PROPERTY_STATE,
        $1:credit_score::INTEGER    AS CREDIT_SCORE,
        $1:ltv_ratio::FLOAT         AS LTV_RATIO,
        $1:dti_ratio::FLOAT         AS DTI_RATIO,
        $1:event_timestamp::TIMESTAMP_NTZ AS EVENT_TIMESTAMP,
        $1:is_valid::BOOLEAN        AS IS_VALID
    FROM @RAW.LOAN_EVENTS_STAGE (FILE_FORMAT => RAW.JSON_FORMAT)
) AS source
ON target.EVENT_ID = source.EVENT_ID
WHEN NOT MATCHED THEN INSERT (
    EVENT_ID, LOAN_ID, BORROWER_ID, TENANT_ID, EVENT_TYPE,
    LOAN_TYPE, LOAN_PURPOSE, ORIGINATION_DATE, MATURITY_DATE,
    ORIGINAL_BALANCE, CURRENT_BALANCE, INTEREST_RATE, MONTHLY_PAYMENT,
    PAYMENT_AMOUNT, DAYS_PAST_DUE, PROPERTY_STATE, CREDIT_SCORE,
    LTV_RATIO, DTI_RATIO, EVENT_TIMESTAMP, IS_VALID
) VALUES (
    source.EVENT_ID, source.LOAN_ID, source.BORROWER_ID, source.TENANT_ID,
    source.EVENT_TYPE, source.LOAN_TYPE, source.LOAN_PURPOSE,
    source.ORIGINATION_DATE, source.MATURITY_DATE, source.ORIGINAL_BALANCE,
    source.CURRENT_BALANCE, source.INTEREST_RATE, source.MONTHLY_PAYMENT,
    source.PAYMENT_AMOUNT, source.DAYS_PAST_DUE, source.PROPERTY_STATE,
    source.CREDIT_SCORE, source.LTV_RATIO, source.DTI_RATIO,
    source.EVENT_TIMESTAMP, source.IS_VALID
);
"""


class SnowflakeLoader:
    """Loads loan event batches into Snowflake with MERGE for idempotency."""

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.conn = None
        self._connect()

    def _connect(self):
        self.conn = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            schema=self.config.schema,
            warehouse=self.config.warehouse,
            role=self.config.role,
            session_parameters={
                'QUERY_TAG': 'loan_ingestion_pipeline',
                'STATEMENT_TIMEOUT_IN_SECONDS': 300
            }
        )
        logger.info(f"Connected to Snowflake: {self.config.account}/{self.config.database}")

    def setup_schema(self):
        """Create raw schema objects if they don't exist."""
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")
            cur.execute("""
                CREATE FILE FORMAT IF NOT EXISTS RAW.JSON_FORMAT
                TYPE = 'JSON'
                COMPRESSION = 'AUTO'
                ENABLE_OCTAL = FALSE
                ALLOW_DUPLICATE = FALSE
                STRIP_OUTER_ARRAY = TRUE
                STRIP_NULL_VALUES = FALSE
                IGNORE_UTF8_ERRORS = FALSE;
            """)
            cur.execute("""
                CREATE STAGE IF NOT EXISTS RAW.LOAN_EVENTS_STAGE
                FILE_FORMAT = RAW.JSON_FORMAT
                COMMENT = 'Stage for raw loan event JSON files';
            """)
            cur.execute(CREATE_RAW_TABLE_SQL)
            logger.info("Schema, stage, and table setup complete.")

    def load_batch(
        self,
        events: List[Dict[str, Any]],
        batch_id: str
    ) -> Dict[str, int]:
        """
        Load a batch of loan events into Snowflake.

        Uses write_pandas for bulk load + MERGE for idempotency.
        Returns dict with rows_inserted, rows_skipped counts.
        """
        if not events:
            return {'rows_inserted': 0, 'rows_skipped': 0}

        df = pd.DataFrame(events)

        # Normalize column names to uppercase (Snowflake convention)
        df.columns = [c.upper() for c in df.columns]
        df['LOADED_AT'] = datetime.utcnow()
        df['BATCH_ID'] = batch_id

        success, nchunks, nrows, _ = write_pandas(
            conn=self.conn,
            df=df,
            table_name='LOAN_EVENTS_STAGING',
            schema='RAW',
            overwrite=True,
            auto_create_table=True
        )

        if not success:
            raise RuntimeError(f"write_pandas failed for batch {batch_id}")

        # MERGE staging into final raw table for idempotency
        with self.conn.cursor(DictCursor) as cur:
            cur.execute(MERGE_SQL)
            result = cur.fetchone()

        rows_inserted = result.get('number of rows inserted', 0) if result else 0
        rows_skipped = len(events) - rows_inserted

        logger.info(
            f"Batch {batch_id}: {rows_inserted} inserted, "
            f"{rows_skipped} duplicates skipped | "
            f"{nchunks} chunks uploaded"
        )

        return {'rows_inserted': rows_inserted, 'rows_skipped': rows_skipped}

    def get_load_stats(self) -> pd.DataFrame:
        """Return load statistics from the raw table."""
        sql = """
            SELECT
                DATE_TRUNC('hour', LOADED_AT)   AS load_hour,
                EVENT_TYPE,
                TENANT_ID,
                COUNT(*)                         AS event_count,
                AVG(ORIGINAL_BALANCE)            AS avg_balance,
                SUM(ORIGINAL_BALANCE)            AS total_balance,
                AVG(CREDIT_SCORE)                AS avg_credit_score,
                AVG(DAYS_PAST_DUE)               AS avg_dpd
            FROM RAW.LOAN_EVENTS
            WHERE LOADED_AT >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
            GROUP BY 1, 2, 3
            ORDER BY 1 DESC, 4 DESC;
        """
        return pd.read_sql(sql, self.conn)

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed.")
