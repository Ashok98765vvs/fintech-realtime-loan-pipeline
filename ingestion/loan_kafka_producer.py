"""
Real-Time Loan Event Producer
==============================
Simulates a real-time mortgage loan event stream for a US fintech client
managing $50B+ in loan portfolios. Produces loan origination, payment,
prepayment, and delinquency events to Kafka / Snowflake Streaming.

Author: Ashok Chowdary
Stack: Python 3.11, Kafka, Snowflake Streaming Ingest API
"""

import json
import time
import uuid
import random
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'loan_events_raw'
EVENT_TYPES = ['ORIGINATION', 'PAYMENT', 'PREPAYMENT', 'DELINQUENCY', 'DEFAULT']
LOAN_TYPES = ['CONVENTIONAL', 'FHA', 'VA', 'JUMBO', 'ARM']
LOAN_PURPOSE = ['PURCHASE', 'REFINANCE', 'CASH_OUT_REFI']
STATES = ['CA', 'TX', 'FL', 'NY', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
TENANT_IDS = ['mortgage_servicer_a', 'mortgage_servicer_b', 'mortgage_servicer_c']


@dataclass
class LoanEvent:
    """Schema for a mortgage loan event."""
    event_id: str
    loan_id: str
    borrower_id: str
    tenant_id: str
    event_type: str
    loan_type: str
    loan_purpose: str
    origination_date: str
    maturity_date: str
    original_balance: float
    current_balance: float
    interest_rate: float
    monthly_payment: float
    payment_amount: Optional[float]
    days_past_due: int
    property_state: str
    credit_score: int
    ltv_ratio: float
    dti_ratio: float
    event_timestamp: str
    is_valid: bool


class LoanEventGenerator:
    """Generates realistic mortgage loan events at configurable throughput."""

    def __init__(self, events_per_second: int = 500):
        self.events_per_second = events_per_second
        self.sleep_interval = 1.0 / events_per_second

    def _generate_loan_id(self) -> str:
        return f"LN-{random.randint(100000, 999999)}-{random.choice(['A','B','C'])}"

    def _generate_origination_date(self) -> str:
        days_ago = random.randint(30, 3650)
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

    def _calculate_monthly_payment(self, balance: float, rate: float, term_months: int) -> float:
        """Standard mortgage payment formula."""
        monthly_rate = rate / 12
        if monthly_rate == 0:
            return balance / term_months
        return balance * (monthly_rate * (1 + monthly_rate) ** term_months) / \
               ((1 + monthly_rate) ** term_months - 1)

    def generate_event(self) -> LoanEvent:
        """Generate a single realistic mortgage loan event."""
        original_balance = round(random.uniform(100_000, 2_500_000), 2)
        interest_rate = round(random.uniform(0.030, 0.085), 4)
        term_months = random.choice([180, 240, 360])  # 15, 20, or 30 year
        months_elapsed = random.randint(0, term_months - 1)
        current_balance = round(original_balance * (1 - months_elapsed / term_months), 2)
        monthly_payment = round(self._calculate_monthly_payment(
            original_balance, interest_rate, term_months), 2)

        event_type = random.choices(
            EVENT_TYPES,
            weights=[0.10, 0.65, 0.10, 0.12, 0.03]
        )[0]

        origination_date = self._generate_origination_date()
        maturity_date = (datetime.strptime(origination_date, '%Y-%m-%d') +
                         timedelta(days=term_months * 30)).strftime('%Y-%m-%d')

        days_past_due = 0
        if event_type == 'DELINQUENCY':
            days_past_due = random.choice([30, 60, 90, 120])
        elif event_type == 'DEFAULT':
            days_past_due = random.randint(120, 365)

        credit_score = int(random.gauss(700, 60))
        credit_score = max(300, min(850, credit_score))

        return LoanEvent(
            event_id=str(uuid.uuid4()),
            loan_id=self._generate_loan_id(),
            borrower_id=f"BRW-{uuid.uuid4().hex[:8].upper()}",
            tenant_id=random.choice(TENANT_IDS),
            event_type=event_type,
            loan_type=random.choice(LOAN_TYPES),
            loan_purpose=random.choice(LOAN_PURPOSE),
            origination_date=origination_date,
            maturity_date=maturity_date,
            original_balance=original_balance,
            current_balance=current_balance,
            interest_rate=interest_rate,
            monthly_payment=monthly_payment,
            payment_amount=round(monthly_payment * random.uniform(0.95, 1.05), 2)
            if event_type == 'PAYMENT' else None,
            days_past_due=days_past_due,
            property_state=random.choice(STATES),
            credit_score=credit_score,
            ltv_ratio=round(current_balance / random.uniform(current_balance, current_balance * 1.5), 4),
            dti_ratio=round(random.uniform(0.15, 0.55), 4),
            event_timestamp=datetime.utcnow().isoformat(),
            is_valid=True
        )


class LoanKafkaProducer:
    """Publishes loan events to Kafka with retry logic and dead-letter queue."""

    def __init__(self, bootstrap_servers: list, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='gzip'
        )
        self.dlq_events = []
        logger.info(f"Kafka producer initialized. Topic: {topic}")

    def _on_send_success(self, record_metadata):
        logger.debug(
            f"Delivered to {record_metadata.topic} "
            f"partition={record_metadata.partition} "
            f"offset={record_metadata.offset}"
        )

    def _on_send_error(self, event: LoanEvent, exc: KafkaError):
        logger.error(f"Failed to deliver event {event.event_id}: {exc}")
        self.dlq_events.append(asdict(event))

    def publish(self, event: LoanEvent):
        """Publish a single loan event to Kafka."""
        event_dict = asdict(event)
        self.producer.send(
            self.topic,
            key=event.tenant_id,
            value=event_dict
        ).add_callback(self._on_send_success).add_errback(
            lambda exc: self._on_send_error(event, exc)
        )

    def flush_dlq(self, dlq_path: str = '/tmp/loan_dlq.jsonl'):
        """Persist dead-letter queue to disk for reprocessing."""
        if self.dlq_events:
            with open(dlq_path, 'a') as f:
                for event in self.dlq_events:
                    f.write(json.dumps(event) + '\n')
            logger.warning(f"Flushed {len(self.dlq_events)} DLQ events to {dlq_path}")
            self.dlq_events.clear()

    def close(self):
        self.producer.flush()
        self.flush_dlq()
        self.producer.close()
        logger.info("Kafka producer closed.")


def run_producer(duration_seconds: int = 3600, events_per_second: int = 500):
    """
    Run the loan event producer for a specified duration.

    Args:
        duration_seconds: How long to run (default: 1 hour)
        events_per_second: Throughput target (default: 500 eps)
    """
    generator = LoanEventGenerator(events_per_second=events_per_second)
    producer = LoanKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC
    )

    start_time = time.time()
    total_events = 0
    batch_size = 100

    logger.info(f"Starting producer: {events_per_second} eps for {duration_seconds}s")

    try:
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()

            for _ in range(batch_size):
                event = generator.generate_event()
                producer.publish(event)
                total_events += 1

            elapsed = time.time() - batch_start
            expected = batch_size / events_per_second
            if elapsed < expected:
                time.sleep(expected - elapsed)

            if total_events % 10_000 == 0:
                logger.info(
                    f"Published {total_events:,} events | "
                    f"Elapsed: {time.time() - start_time:.1f}s | "
                    f"DLQ size: {len(producer.dlq_events)}"
                )

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.close()
        logger.info(f"Total events published: {total_events:,}")


if __name__ == '__main__':
    run_producer(duration_seconds=3600, events_per_second=500)
