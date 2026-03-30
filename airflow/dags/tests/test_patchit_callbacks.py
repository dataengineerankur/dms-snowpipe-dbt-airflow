"""Tests for patchit_callbacks.py

These tests are intentionally free of any Airflow dependency so they run in a
lightweight CI environment where only the stdlib and pytest are installed.  All
Airflow objects that would normally appear in the callback *context* dict are
replaced with plain MagicMock instances.
"""
from __future__ import annotations

import io
import json
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Make patchit_callbacks importable even when Airflow is not installed by
# inserting the dags directory on sys.path before importing.
# ---------------------------------------------------------------------------
import os
import importlib

_DAGS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from patchit_callbacks import patchit_failure_callback  # noqa: E402


def _make_context(
    dag_id: str = "test_dag",
    task_id: str = "test_task",
    run_id: str = "manual__2026-01-01T00:00:00",
    execution_date: datetime | None = None,
    exception: BaseException | None = None,
    log_url: str | None = "http://airflow/log/test_dag/test_task/1",
) -> dict:
    """Build a minimal Airflow-style context dict using plain MagicMocks."""
    dag_mock = MagicMock()
    dag_mock.dag_id = dag_id

    ti_mock = MagicMock()
    ti_mock.task_id = task_id
    ti_mock.log_url = log_url

    return {
        "dag": dag_mock,
        "task_instance": ti_mock,
        "run_id": run_id,
        "execution_date": execution_date or datetime(2026, 1, 1, 0, 0, 0),
        "exception": exception,
    }


def _capture_patchit_evidence(context: dict) -> dict:
    """
    Call patchit_failure_callback with *context*, capture stdout, and return
    the parsed JSON payload from the ``PATCHIT_EVIDENCE:`` line.
    Raises AssertionError if no such line is found.
    """
    buf = io.StringIO()
    with patch("sys.stdout", buf):
        patchit_failure_callback(context)

    output = buf.getvalue()
    for line in output.splitlines():
        if line.startswith("PATCHIT_EVIDENCE:"):
            payload = line[len("PATCHIT_EVIDENCE:"):].strip()
            return json.loads(payload)

    raise AssertionError(
        f"No PATCHIT_EVIDENCE: line found in callback output.\n"
        f"Captured stdout:\n{output}"
    )


class TestPatchitFailureCallback(unittest.TestCase):
    # ------------------------------------------------------------------ #
    # Test 1: happy path — exception present                               #
    # ------------------------------------------------------------------ #
    def test_callback_emits_patchit_evidence_line(self) -> None:
        """Callback must emit a PATCHIT_EVIDENCE: JSON line with correct fields."""
        exc = ValueError("dbt run failed: model stg_orders")
        context = _make_context(
            dag_id="test_dag",
            task_id="test_task",
            run_id="manual__2026-01-01T00:00:00",
            execution_date=datetime(2026, 1, 1, 0, 0, 0),
            exception=exc,
            log_url="http://airflow/log/test_dag/test_task/1",
        )

        buf = io.StringIO()
        with patch("sys.stdout", buf):
            patchit_failure_callback(context)

        output = buf.getvalue()

        # Assert the raw prefix is present
        self.assertIn("PATCHIT_EVIDENCE:", output, "Output must contain PATCHIT_EVIDENCE: prefix")

        # Parse the JSON payload
        evidence = _capture_patchit_evidence(context)

        # Structural checks
        self.assertEqual(evidence["level"], "ERROR")
        self.assertEqual(evidence["event"], "airflow_task_failure")
        self.assertEqual(evidence["dag_id"], "test_dag")
        self.assertEqual(evidence["task_id"], "test_task")
        self.assertEqual(evidence["run_id"], "manual__2026-01-01T00:00:00")
        self.assertIsNotNone(evidence["execution_date"])
        self.assertEqual(evidence["exception_type"], "ValueError")
        self.assertIn("dbt run failed", evidence["exception_msg"])
        self.assertIsNotNone(evidence["traceback_tail"])
        self.assertEqual(evidence["log_url"], "http://airflow/log/test_dag/test_task/1")

    # ------------------------------------------------------------------ #
    # Test 2: edge case — exception is None                                #
    # ------------------------------------------------------------------ #
    def test_callback_handles_none_exception(self) -> None:
        """Callback must not raise when context['exception'] is None and must
        still emit a PATCHIT_EVIDENCE: line."""
        context = _make_context(exception=None)

        buf = io.StringIO()
        # Should not raise
        with patch("sys.stdout", buf):
            patchit_failure_callback(context)

        output = buf.getvalue()
        self.assertIn(
            "PATCHIT_EVIDENCE:",
            output,
            "Output must contain PATCHIT_EVIDENCE: prefix even when exception is None",
        )

        evidence = _capture_patchit_evidence(context)
        self.assertIsNone(evidence["exception_type"])
        self.assertIsNone(evidence["exception_msg"])
        self.assertIsNone(evidence["traceback_tail"])
        self.assertEqual(evidence["event"], "airflow_task_failure")

    # ------------------------------------------------------------------ #
    # Test 3: execution_date coercion                                      #
    # ------------------------------------------------------------------ #
    def test_execution_date_is_serialised_as_string(self) -> None:
        """execution_date must be a string in the evidence dict (ISO 8601)."""
        context = _make_context(execution_date=datetime(2026, 3, 30, 12, 0, 0))
        evidence = _capture_patchit_evidence(context)
        self.assertIsInstance(evidence["execution_date"], str)
        self.assertTrue(
            evidence["execution_date"].startswith("2026-03-30"),
            f"Unexpected execution_date format: {evidence['execution_date']}",
        )

    # ------------------------------------------------------------------ #
    # Test 4: missing log_url (task_instance has no log_url attribute)     #
    # ------------------------------------------------------------------ #
    def test_callback_handles_missing_log_url(self) -> None:
        """Callback must set log_url=None when task_instance lacks the attribute."""
        context = _make_context()
        # Remove the log_url attribute from the mock
        del context["task_instance"].log_url

        evidence = _capture_patchit_evidence(context)
        self.assertIsNone(evidence["log_url"])

    # ------------------------------------------------------------------ #
    # Test 5: evidence dict contains all required keys                     #
    # ------------------------------------------------------------------ #
    def test_evidence_dict_contains_all_required_keys(self) -> None:
        """Every field in the PATCHIT evidence spec must be present."""
        required_keys = {
            "level",
            "event",
            "dag_id",
            "task_id",
            "run_id",
            "execution_date",
            "exception_type",
            "exception_msg",
            "traceback_tail",
            "log_url",
        }
        context = _make_context()
        evidence = _capture_patchit_evidence(context)
        missing = required_keys - set(evidence.keys())
        self.assertFalse(missing, f"Missing keys in evidence dict: {missing}")


if __name__ == "__main__":
    unittest.main()
