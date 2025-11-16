"""
Simple Dagster jobs for testing the Kestra plugin
"""
from dagster import job, op, repository, Definitions
import time


@op
def hello_op(context):
    """Simple operation that logs a message"""
    context.log.info("Hello from Dagster!")
    return "Hello"


@op
def wait_op(context, input_value: str):
    """Operation that waits a bit to simulate work"""
    context.log.info(f"Processing: {input_value}")
    time.sleep(1)
    return f"Processed: {input_value}"


@job(name="test_job")
def test_job_definition():
    """Simple job for testing without wait"""
    result = hello_op()
    wait_op(result)


@job(name="test_job_wait")
def test_job_wait_definition():
    """Job for testing with wait for completion"""
    result = hello_op()
    wait_op(result)


# Define the repository
defs = Definitions(
    jobs=[test_job_definition, test_job_wait_definition]
)
