import pytest

from stream_processor import TelecomStreamProcessor


# ---------------------------------------------------------------------------
# Session-scoped fixtures — one SparkSession and one pair of DataFrames for
# the entire test run (avoids the overhead of re-creating them per test).
#
# DataFrames are built with Spark SQL VALUES syntax so that no Python-side
# serialization is needed (avoids a PySpark 3.3 / Python 3.12 cloudpickle
# incompatibility that occurs with spark.createDataFrame + Python lists).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def processor():
    return TelecomStreamProcessor()


@pytest.fixture(scope="session")
def monitoring_df(processor):
    """
    Monitoring events (mirrors monitoring_stream.csv):

        time,service_id,status
        2021-04-14T08:09:43Z,igf-serv-10000,UP
        2021-04-14T08:09:45Z,igf-serv-10000,DOWN
        2021-04-14T08:09:47Z,igf-serv-10000,DOWN
        2021-04-14T08:09:49Z,igf-serv-10000,UP
        2021-04-14T08:09:51Z,igf-serv-10000,DOWN
        2021-04-14T08:09:52Z,igf-serv-10000,UP
        2021-04-14T08:09:55Z,igf-serv-10001,UP
        2021-04-14T08:09:57Z,igf-serv-10001,DOWN
        2021-04-14T08:10:00Z,igf-serv-10001,UP
    """
    return processor.spark.sql("""
        SELECT to_timestamp(time) AS time, service_id, status
        FROM VALUES
            ('2021-04-14T08:09:43Z', 'igf-serv-10000', 'UP'),
            ('2021-04-14T08:09:45Z', 'igf-serv-10000', 'DOWN'),
            ('2021-04-14T08:09:47Z', 'igf-serv-10000', 'DOWN'),
            ('2021-04-14T08:09:49Z', 'igf-serv-10000', 'UP'),
            ('2021-04-14T08:09:51Z', 'igf-serv-10000', 'DOWN'),
            ('2021-04-14T08:09:52Z', 'igf-serv-10000', 'UP'),
            ('2021-04-14T08:09:55Z', 'igf-serv-10001', 'UP'),
            ('2021-04-14T08:09:57Z', 'igf-serv-10001', 'DOWN'),
            ('2021-04-14T08:10:00Z', 'igf-serv-10001', 'UP')
        AS t(time, service_id, status)
    """)


@pytest.fixture(scope="session")
def provisioning_df(processor):
    """
    Provisioning events (mirrors provisioning_stream.csv).
    The row at 08:09:50 has NULL customer_id/segment, representing an
    unprovisioning event.

        time,service_id,customer_id,segment
        2021-04-14T08:09:41Z,igf-serv-10000,c1,RESIDENTIAL
        2021-04-14T08:09:46Z,igf-serv-10000,c1,BUSINESS
        2021-04-14T08:09:50Z,igf-serv-10000,,
        2021-04-14T08:09:51Z,igf-serv-10000,c1,BUSINESS
        2021-04-14T08:09:56Z,igf-serv-10001,c2,RESIDENTIAL
    """
    return processor.spark.sql("""
        SELECT to_timestamp(time) AS time, service_id, customer_id, segment
        FROM VALUES
            ('2021-04-14T08:09:41Z', 'igf-serv-10000', 'c1',   'RESIDENTIAL'),
            ('2021-04-14T08:09:46Z', 'igf-serv-10000', 'c1',   'BUSINESS'),
            ('2021-04-14T08:09:50Z', 'igf-serv-10000', NULL,    NULL),
            ('2021-04-14T08:09:51Z', 'igf-serv-10000', 'c1',   'BUSINESS'),
            ('2021-04-14T08:09:56Z', 'igf-serv-10001', 'c2',   'RESIDENTIAL')
        AS t(time, service_id, customer_id, segment)
    """)


# ---------------------------------------------------------------------------
# Task A — customers experiencing downtime at a given time
# ---------------------------------------------------------------------------


class TestTaskA:
    def test_no_downtime_at_08_09_52(self, processor, monitoring_df, provisioning_df):
        """
        At 08:09:52 igf-serv-10000 transitions to UP (event at 08:09:52) and
        igf-serv-10001 has no events yet → no customer is in downtime.
        """
        result = processor.get_customers_in_downtime(
            monitoring_df, provisioning_df, "2021-04-14T08:09:52Z"
        )
        assert result.count() == 0

    def test_c2_in_downtime_at_08_09_58(self, processor, monitoring_df, provisioning_df):
        """
        At 08:09:58 igf-serv-10001 is DOWN (last event at 08:09:57) and is
        provisioned to c2 (RESIDENTIAL, provisioned at 08:09:56).
        """
        result = processor.get_customers_in_downtime(
            monitoring_df, provisioning_df, "2021-04-14T08:09:58Z"
        )
        rows = result.collect()
        assert len(rows) == 1
        row = rows[0]
        assert row["customer_id"] == "c2"
        assert row["service_id"] == "igf-serv-10001"
        assert row["segment"] == "RESIDENTIAL"

    def test_c1_business_in_downtime_at_08_09_46(self, processor, monitoring_df, provisioning_df):
        """
        At 08:09:46 igf-serv-10000 is DOWN (last event at 08:09:45) and the
        latest provisioning entry at ≤08:09:46 is c1 / BUSINESS (provisioned
        exactly at 08:09:46).
        """
        result = processor.get_customers_in_downtime(
            monitoring_df, provisioning_df, "2021-04-14T08:09:46Z"
        )
        rows = result.collect()
        assert len(rows) == 1
        row = rows[0]
        assert row["customer_id"] == "c1"
        assert row["service_id"] == "igf-serv-10000"
        assert row["segment"] == "BUSINESS"

    def test_c1_in_downtime_at_08_09_51(self, processor, monitoring_df, provisioning_df):
        """
        At 08:09:51 igf-serv-10000 goes DOWN and is simultaneously
        re-provisioned to c1 (BUSINESS) — the service is in downtime and
        the customer is c1.
        """
        result = processor.get_customers_in_downtime(
            monitoring_df, provisioning_df, "2021-04-14T08:09:51Z"
        )
        rows = result.collect()
        assert len(rows) == 1
        row = rows[0]
        assert row["customer_id"] == "c1"
        assert row["service_id"] == "igf-serv-10000"


# ---------------------------------------------------------------------------
# Task B — mean duration of disturbance for business customers
# ---------------------------------------------------------------------------


class TestTaskB:
    def test_mean_business_downtime_at_08_09_52(self, processor, monitoring_df, provisioning_df):
        """
        At query_time=08:09:52 there is exactly one BUSINESS downtime period:
            igf-serv-10000  08:09:51 → 08:09:52  (1 second, segment=BUSINESS)
        The first period (08:09:45→08:09:49, 4 s) was provisioned as RESIDENTIAL.
        Expected: mean = 1.0 s, max = 1.0 s.
        """
        _, stats = processor.get_business_downtime_stats(
            monitoring_df, provisioning_df, "2021-04-14T08:09:52Z"
        )
        row = stats.collect()[0]
        assert row["mean_duration_seconds"] == pytest.approx(1.0)
        assert row["max_duration_seconds"] == pytest.approx(1.0)

    def test_mean_business_downtime_at_08_10_00(self, processor, monitoring_df, provisioning_df):
        """
        At query_time=08:10:00 an additional period is added for igf-serv-10001:
            igf-serv-10001  08:09:57 → 08:10:00  (3 s, segment=RESIDENTIAL)
        That period is RESIDENTIAL so it doesn't affect the BUSINESS mean.
        Expected: mean = 1.0 s (unchanged).
        """
        _, stats = processor.get_business_downtime_stats(
            monitoring_df, provisioning_df, "2021-04-14T08:10:00Z"
        )
        row = stats.collect()[0]
        assert row["mean_duration_seconds"] == pytest.approx(1.0)
