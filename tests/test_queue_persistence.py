import asyncio
import json
import tempfile
import unittest
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import main


class QueuePersistenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        self.original_values = {
            "history_path": main.history_path,
            "queues": main.queues,
            "locks": main.locks,
            "scheduled_tasks": main.scheduled_tasks,
            "scheduled_timer_tasks": main.scheduled_timer_tasks,
            "scheduled_tasks_lock": main.scheduled_tasks_lock,
        }
        main.history_path = temp_path / "conversation_log.jsonl"
        main.queues = {
            queue_name: deque()
            for queue_name in main.QUEUE_DEFINITIONS
        }
        main.locks = {
            queue_name: asyncio.Lock()
            for queue_name in main.QUEUE_DEFINITIONS
        }
        main.scheduled_tasks = {}
        main.scheduled_timer_tasks = {}
        main.scheduled_tasks_lock = asyncio.Lock()

    async def asyncTearDown(self) -> None:
        await main.shutdown_runtime_state()
        for name, value in self.original_values.items():
            setattr(main, name, value)
        self.temp_dir.cleanup()

    async def simulate_restart(self) -> None:
        await main.shutdown_runtime_state()
        main.queues = {
            queue_name: deque()
            for queue_name in main.QUEUE_DEFINITIONS
        }
        main.scheduled_tasks = {}
        main.scheduled_timer_tasks = {}
        await main.restore_runtime_state()

    async def test_queue_survives_restore_and_dequeue_is_persisted(self) -> None:
        git_context = {
            "git_context_key": "github.com/example/delta",
            "git_address": "https://github.com/example/delta.git",
        }
        queued = await main.enqueue_phone_channel(
            "worker-all",
            "9008",
            "Persistent task",
            {"from_phone": "0001", "to_phone": "2101"},
            git_context=git_context,
        )

        state_path = main.queue_runtime_state_path("worker-all")
        self.assertTrue(state_path.exists())
        persisted = main.read_queue_runtime_state("worker-all")
        self.assertEqual([item["id"] for item in persisted], [queued["id"]])

        main.queues = {
            queue_name: deque()
            for queue_name in main.QUEUE_DEFINITIONS
        }
        await main.restore_runtime_state()
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(main.queue_item_message(main.queues["worker-all"][0]), "Persistent task")

        delivered = await main.dequeue_phone_channel(
            "worker-all",
            "9008",
            "2101",
            git_context=git_context,
        )
        self.assertEqual(delivered["id"], queued["id"])
        self.assertEqual(main.read_queue_runtime_state("worker-all"), [])

        await main.restore_runtime_state()
        self.assertEqual(len(main.queues["worker-all"]), 0)

    async def test_restored_queue_keeps_git_projects_isolated(self) -> None:
        delta_context = {
            "git_context_key": "github.com/example/delta",
            "git_address": "https://github.com/example/delta.git",
        }
        other_context = {
            "git_context_key": "github.com/example/other",
            "git_address": "https://github.com/example/other.git",
        }
        await main.enqueue_phone_channel(
            "worker-all",
            "9008",
            "Delta task",
            {"from_phone": "0001", "to_phone": "2101"},
            git_context=delta_context,
        )
        await main.enqueue_phone_channel(
            "worker-all",
            "9008",
            "Other project task",
            {"from_phone": "0001", "to_phone": "2101"},
            git_context=other_context,
        )

        main.queues["worker-all"].clear()
        await main.restore_runtime_state()
        delivered = await main.dequeue_phone_channel(
            "worker-all",
            "9008",
            "2101",
            git_context=delta_context,
        )
        self.assertEqual(delivered["message"], "Delta task")
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(
            main.queue_item_message(main.queues["worker-all"][0]),
            "Other project task",
        )

        main.queues["worker-all"].clear()
        await main.restore_runtime_state()
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(
            main.queue_item_message(main.queues["worker-all"][0]),
            "Other project task",
        )

    async def test_failed_disk_write_rolls_back_enqueue(self) -> None:
        with patch.object(
            main,
            "write_queue_runtime_state",
            side_effect=OSError("disk unavailable"),
        ):
            with self.assertRaises(OSError):
                await main.enqueue(
                    "work",
                    "Must not remain only in memory",
                    git_context={
                        "git_context_key": "github.com/example/delta",
                        "git_address": "https://github.com/example/delta.git",
                    },
                )
        self.assertEqual(len(main.queues["work"]), 0)

    async def test_scheduled_task_survives_restore(self) -> None:
        scheduled = await main.schedule_message(
            "worker-all",
            "Release after PASS",
            {"from_phone": "0001", "to_phone": "2101"},
            schedule={"mode": "pass"},
            git_context={
                "git_context_key": "github.com/example/delta",
                "git_address": "https://github.com/example/delta.git",
            },
        )
        task_id = scheduled["id"]
        self.assertIn(task_id, main.read_scheduled_runtime_state())

        main.scheduled_tasks.clear()
        await main.restore_runtime_state()
        self.assertIn(task_id, main.scheduled_tasks)

        await main.cancel_scheduled_task(task_id)
        self.assertNotIn(task_id, main.read_scheduled_runtime_state())

    async def test_overdue_scheduled_task_releases_once_after_restart(self) -> None:
        scheduled = await main.schedule_message(
            "worker-all",
            "Overdue task",
            {"from_phone": "0001", "to_phone": "2101"},
            schedule={"mode": "delay", "delay_minutes": 5},
            git_context={
                "git_context_key": "github.com/example/delta",
                "git_address": "https://github.com/example/delta.git",
            },
        )
        task_id = scheduled["id"]
        async with main.scheduled_tasks_lock:
            main.scheduled_tasks[task_id]["due_at"] = (
                datetime.now(timezone.utc) - timedelta(seconds=1)
            ).isoformat()
            await main.persist_scheduled_tasks_locked()

        await self.simulate_restart()
        for _ in range(100):
            if task_id not in main.scheduled_tasks:
                break
            await asyncio.sleep(0.01)

        self.assertNotIn(task_id, main.scheduled_tasks)
        matching = [
            item
            for item in main.queues["worker-all"]
            if main.queue_item_metadata(item).get("scheduled_task_id") == task_id
        ]
        self.assertEqual(len(matching), 1)

        queued_id = main.queue_item_id(matching[0])
        await self.simulate_restart()
        matching_after_restart = [
            item
            for item in main.queues["worker-all"]
            if main.queue_item_metadata(item).get("scheduled_task_id") == task_id
        ]
        self.assertEqual(
            [main.queue_item_id(item) for item in matching_after_restart],
            [queued_id],
        )

    async def test_restore_reconciles_release_persisted_before_schedule_cleanup(self) -> None:
        scheduled = await main.schedule_message(
            "worker-all",
            "Crash-window task",
            {"from_phone": "0001", "to_phone": "2101"},
            schedule={"mode": "pass"},
            git_context={
                "git_context_key": "github.com/example/delta",
                "git_address": "https://github.com/example/delta.git",
            },
        )
        task_id = scheduled["id"]
        await main.enqueue(
            "worker-all",
            "Crash-window task",
            {
                "from_phone": "0001",
                "to_phone": "2101",
                "scheduled_release": True,
                "scheduled_task_id": task_id,
            },
            git_context={
                "git_context_key": "github.com/example/delta",
                "git_address": "https://github.com/example/delta.git",
            },
        )

        await self.simulate_restart()

        self.assertNotIn(task_id, main.scheduled_tasks)
        self.assertNotIn(task_id, main.read_scheduled_runtime_state())
        matching = [
            item
            for item in main.queues["worker-all"]
            if main.queue_item_metadata(item).get("scheduled_task_id") == task_id
        ]
        self.assertEqual(len(matching), 1)

    def test_corrupt_queue_state_fails_closed(self) -> None:
        state_path = main.queue_runtime_state_path("worker-all")
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
        with self.assertRaises(RuntimeError):
            main.read_queue_runtime_state("worker-all")


if __name__ == "__main__":
    unittest.main()
