import asyncio
import threading
import unittest
from unittest.mock import Mock, patch

import main
from nginx_qa import telegram_launcher_controls as controls
from nginx_qa import telegram_launcher_intake as intake


class TelegramDeliveryLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_delivery_is_independent_of_executor_wait_and_both_drain_at_shutdown(self):
        stop=threading.Event()
        started=[asyncio.Event(),asyncio.Event()]
        release=[threading.Event(),threading.Event()]
        counts=[0,0]
        async def recover(index,host,**kwargs):
            counts[index]+=1
            started[index].set()
            await asyncio.to_thread(release[index].wait,5)
        async def launch_recovery(host,**kwargs):
            await recover(0,host,**kwargs)
        async def notice_recovery(host,**kwargs):
            await recover(1,host,**kwargs)
        runtime=Mock()
        with patch.object(intake,'reconcile_operations',side_effect=launch_recovery), \
             patch.object(controls,'reconcile_notifications',side_effect=notice_recovery):
            thread=threading.Thread(target=main._reconcile_managed_continuity_background,
                args=(runtime,stop,0.005,asyncio.get_running_loop()),daemon=True)
            thread.start()
            try:
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)),2)
                await asyncio.sleep(0.05)
                self.assertEqual([1,1],counts)
                self.assertGreater(runtime.reconcile_all.call_count,2)
                stop.set()
                release[0].set()
                await asyncio.to_thread(thread.join,0.03)
                self.assertTrue(thread.is_alive(),'the provider call still owns shutdown resources')
                release[1].set()
                await asyncio.to_thread(thread.join,2)
                self.assertFalse(thread.is_alive())
            finally:
                stop.set()
                for event in release:
                    event.set()
                await asyncio.to_thread(thread.join,2)
