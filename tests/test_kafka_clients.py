"""Guards against passing invalid aiokafka client arguments.

aiokafka clients must be constructed inside a running event loop (as the
services do in their lifespans), so these tests build them inside asyncio.run.
This catches signature errors that would otherwise only appear at start-up.
"""

import asyncio

from kafka.consumer import build_consumer
from kafka.producer import build_producer


def test_producer_accepts_the_configured_options():
    async def build():
        return build_producer()

    assert asyncio.run(build()) is not None


def test_consumer_accepts_the_configured_options():
    async def build():
        return build_consumer("test-instance")

    assert asyncio.run(build()) is not None


def test_topic_provisioning_api_surface():
    """The collector provisions topics on start-up, so the admin API we call
    must exist with the arguments we pass."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    topic = NewTopic(name="app-performance-metrics", num_partitions=12, replication_factor=1)
    assert topic.name == "app-performance-metrics"
    assert topic.num_partitions == 12
    for method in ("start", "create_topics", "close"):
        assert hasattr(AIOKafkaAdminClient, method), f"admin client is missing {method}"
    assert issubclass(TopicAlreadyExistsError, Exception)
