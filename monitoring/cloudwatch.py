"""
monitoring/cloudwatch.py
------------------------
Sets up a CloudWatch dashboard with the key pipeline metrics.
Run once after deploying to get a single-pane-of-glass view.

Usage:
    python monitoring/cloudwatch.py --stack kinesis-pipeline-dev
"""

import argparse
import json
import logging
import os

import boto3

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def build_dashboard_body(
    stream_name: str,
    function_name: str,
    firehose_name: str,
    dlq_name: str,
) -> str:
    widgets = [
        # ── Kinesis throughput ──────────────────────────────────────────
        {
            "type": "metric",
            "properties": {
                "title": "Kinesis — IncomingRecords/s",
                "metrics": [[
                    "AWS/Kinesis", "IncomingRecords",
                    "StreamName", stream_name,
                    {"stat": "Sum", "period": 60}
                ]],
                "view": "timeSeries",
                "region": AWS_REGION,
            },
        },
        # ── Lambda invocations + errors ─────────────────────────────────
        {
            "type": "metric",
            "properties": {
                "title": "Lambda — Invocations & Errors",
                "metrics": [
                    ["AWS/Lambda", "Invocations", "FunctionName", function_name,
                     {"stat": "Sum", "period": 60}],
                    ["AWS/Lambda", "Errors", "FunctionName", function_name,
                     {"stat": "Sum", "period": 60, "color": "#d62728"}],
                ],
                "view": "timeSeries",
                "region": AWS_REGION,
            },
        },
        # ── Lambda duration ─────────────────────────────────────────────
        {
            "type": "metric",
            "properties": {
                "title": "Lambda — Duration (P50 / P99)",
                "metrics": [
                    ["AWS/Lambda", "Duration", "FunctionName", function_name,
                     {"stat": "p50", "period": 60, "label": "P50"}],
                    ["AWS/Lambda", "Duration", "FunctionName", function_name,
                     {"stat": "p99", "period": 60, "label": "P99", "color": "#ff7f0e"}],
                ],
                "view": "timeSeries",
                "region": AWS_REGION,
            },
        },
        # ── Firehose data freshness ─────────────────────────────────────
        {
            "type": "metric",
            "properties": {
                "title": "Firehose — DeliveryToS3.DataFreshness (s)",
                "metrics": [[
                    "AWS/Firehose", "DeliveryToS3.DataFreshness",
                    "DeliveryStreamName", firehose_name,
                    {"stat": "Maximum", "period": 60}
                ]],
                "view": "timeSeries",
                "region": AWS_REGION,
            },
        },
        # ── SQS DLQ depth ───────────────────────────────────────────────
        {
            "type": "metric",
            "properties": {
                "title": "SQS DLQ — Messages Visible",
                "metrics": [[
                    "AWS/SQS", "ApproximateNumberOfMessagesVisible",
                    "QueueName", dlq_name,
                    {"stat": "Maximum", "period": 300, "color": "#d62728"}
                ]],
                "view": "timeSeries",
                "region": AWS_REGION,
            },
        },
    ]

    return json.dumps({"widgets": widgets})


def create_or_update_dashboard(
    dashboard_name: str,
    stream_name: str,
    function_name: str,
    firehose_name: str,
    dlq_name: str,
) -> None:
    client = boto3.client("cloudwatch", region_name=AWS_REGION)
    body = build_dashboard_body(stream_name, function_name, firehose_name, dlq_name)

    client.put_dashboard(DashboardName=dashboard_name, DashboardBody=body)
    log.info(
        "Dashboard '%s' created/updated: "
        "https://%s.console.aws.amazon.com/cloudwatch/home#dashboards:name=%s",
        dashboard_name,
        AWS_REGION,
        dashboard_name,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create CloudWatch pipeline dashboard")
    parser.add_argument("--stack", default="kinesis-pipeline-dev", help="Stack name prefix")
    parser.add_argument("--dashboard", default="kinesis-pipeline", help="Dashboard name")
    args = parser.parse_args()

    create_or_update_dashboard(
        dashboard_name=args.dashboard,
        stream_name=f"{args.stack}-events",
        function_name=f"{args.stack}-transformer",
        firehose_name=f"{args.stack}-events-firehose",
        dlq_name=f"{args.stack}-events-dlq",
    )


if __name__ == "__main__":
    main()
