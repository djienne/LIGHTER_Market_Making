"""Tests for the _paced_send mailbox drain semantics.

The sender must:
- pull the freshest ops AFTER the pacing gate (a long rate-limit wait must
  never send prices computed before the wait),
- keep draining ops posted while a send was in flight (no waiting for the
  next book tick),
- drop ops cleanly when the write slot is denied.
"""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


def _op(label, action="create"):
    return SimpleNamespace(label=label, action=action)


class TestPacedSendMailbox(unittest.IsolatedAsyncioTestCase):

    async def test_freshest_ops_win_after_pacing_wait(self):
        """Ops replaced in the mailbox during the pacing wait are the ones sent."""
        gate = asyncio.Event()
        sent = []

        async def fake_slot(**kwargs):
            await gate.wait()
            return True

        async def fake_send(client, ops):
            sent.append(ops)

        with temp_mm_attrs(_latest_ops=None, _send_task=None):
            with patch.object(mm, "_wait_for_write_slot", new=fake_slot), \
                 patch.object(mm, "sign_and_send_batch", new=fake_send):
                mm._latest_ops = [_op("stale")]
                task = asyncio.create_task(mm._paced_send(None))
                await asyncio.sleep(0.05)  # sender parked at the pacing gate

                mm._latest_ops = [_op("fresh")]  # newer quote arrives meanwhile
                gate.set()
                await asyncio.wait_for(task, timeout=1.0)

            self.assertEqual(len(sent), 1)
            self.assertEqual(sent[0][0].label, "fresh")
            self.assertIsNone(mm._latest_ops)

    async def test_ops_posted_while_sending_are_drained(self):
        """A batch posted mid-send goes out from the same sender task —
        it must not wait for the next book event to be dispatched."""
        first_send_started = asyncio.Event()
        release_first_send = asyncio.Event()
        sent = []

        async def fake_slot(**kwargs):
            return True

        async def fake_send(client, ops):
            sent.append(ops)
            if len(sent) == 1:
                first_send_started.set()
                await release_first_send.wait()

        with temp_mm_attrs(_latest_ops=None, _send_task=None):
            with patch.object(mm, "_wait_for_write_slot", new=fake_slot), \
                 patch.object(mm, "sign_and_send_batch", new=fake_send):
                mm._latest_ops = [_op("first")]
                task = asyncio.create_task(mm._paced_send(None))
                await asyncio.wait_for(first_send_started.wait(), timeout=1.0)

                mm._latest_ops = [_op("second")]  # posted while send in flight
                release_first_send.set()
                await asyncio.wait_for(task, timeout=1.0)

            self.assertEqual([batch[0].label for batch in sent], ["first", "second"])
            self.assertIsNone(mm._latest_ops)

    async def test_denied_write_slot_drops_ops(self):
        """Slot denied: nothing sent, mailbox cleared (ops recomputed next tick)."""
        sent = []

        async def fake_slot(**kwargs):
            return False

        async def fake_send(client, ops):
            sent.append(ops)

        with temp_mm_attrs(_latest_ops=None, _send_task=None):
            with patch.object(mm, "_wait_for_write_slot", new=fake_slot), \
                 patch.object(mm, "sign_and_send_batch", new=fake_send):
                mm._latest_ops = [_op("doomed")]
                await asyncio.wait_for(mm._paced_send(None), timeout=1.0)

            self.assertEqual(sent, [])
            self.assertIsNone(mm._latest_ops)

    async def test_empty_mailbox_returns_immediately(self):
        slot_calls = []

        async def fake_slot(**kwargs):
            slot_calls.append(1)
            return True

        with temp_mm_attrs(_latest_ops=None, _send_task=None):
            with patch.object(mm, "_wait_for_write_slot", new=fake_slot):
                await asyncio.wait_for(mm._paced_send(None), timeout=1.0)

        self.assertEqual(slot_calls, [])

    async def test_cancel_only_flag_passed_to_gate(self):
        seen_flags = []

        async def fake_slot(**kwargs):
            seen_flags.append(kwargs.get("cancel_only"))
            return True

        async def fake_send(client, ops):
            pass

        with temp_mm_attrs(_latest_ops=None, _send_task=None):
            with patch.object(mm, "_wait_for_write_slot", new=fake_slot), \
                 patch.object(mm, "sign_and_send_batch", new=fake_send):
                mm._latest_ops = [_op("c1", action="cancel"), _op("c2", action="cancel")]
                await asyncio.wait_for(mm._paced_send(None), timeout=1.0)

        self.assertEqual(seen_flags, [True])


if __name__ == "__main__":
    unittest.main()
