# Copyright 2014 Hewlett-Packard Development Company, L.P.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

import datetime

from oslo_utils import uuidutils

from neutron.agent.common import resource_processing_queue as queue
from neutron.tests import base

_uuid = uuidutils.generate_uuid
FAKE_ID = _uuid()
FAKE_ID_2 = _uuid()

PRIORITY_RPC = 0


class TestExclusiveResourceProcessor(base.BaseTestCase):

    def test_i_am_main(self):
        main = queue.ExclusiveResourceProcessor(FAKE_ID)
        not_main = queue.ExclusiveResourceProcessor(FAKE_ID)
        main_2 = queue.ExclusiveResourceProcessor(FAKE_ID_2)
        not_main_2 = queue.ExclusiveResourceProcessor(FAKE_ID_2)

        self.assertTrue(main._i_am_main())
        self.assertFalse(not_main._i_am_main())
        self.assertTrue(main_2._i_am_main())
        self.assertFalse(not_main_2._i_am_main())

        main.__exit__(None, None, None)
        main_2.__exit__(None, None, None)

    def test_main(self):
        main = queue.ExclusiveResourceProcessor(FAKE_ID)
        not_main = queue.ExclusiveResourceProcessor(FAKE_ID)
        main_2 = queue.ExclusiveResourceProcessor(FAKE_ID_2)
        not_main_2 = queue.ExclusiveResourceProcessor(FAKE_ID_2)

        self.assertEqual(main, main._main)
        self.assertEqual(main, not_main._main)
        self.assertEqual(main_2, main_2._main)
        self.assertEqual(main_2, not_main_2._main)

        main.__exit__(None, None, None)
        main_2.__exit__(None, None, None)

    def test__enter__(self):
        self.assertNotIn(FAKE_ID, queue.ExclusiveResourceProcessor._mains)
        main = queue.ExclusiveResourceProcessor(FAKE_ID)
        main.__enter__()
        self.assertIn(FAKE_ID, queue.ExclusiveResourceProcessor._mains)
        main.__exit__(None, None, None)

    def test__exit__(self):
        main = queue.ExclusiveResourceProcessor(FAKE_ID)
        not_main = queue.ExclusiveResourceProcessor(FAKE_ID)
        main.__enter__()
        self.assertIn(FAKE_ID, queue.ExclusiveResourceProcessor._mains)
        not_main.__enter__()
        not_main.__exit__(None, None, None)
        self.assertIn(FAKE_ID, queue.ExclusiveResourceProcessor._mains)
        main.__exit__(None, None, None)
        self.assertNotIn(FAKE_ID, queue.ExclusiveResourceProcessor._mains)

    def test_data_fetched_since(self):
        main = queue.ExclusiveResourceProcessor(FAKE_ID)
        self.assertEqual(datetime.datetime.min,
                         main._get_resource_data_timestamp())

        ts1 = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)
        ts2 = datetime.datetime.utcnow()

        main.fetched_and_processed(ts2)
        self.assertEqual(ts2, main._get_resource_data_timestamp())
        main.fetched_and_processed(ts1)
        self.assertEqual(ts2, main._get_resource_data_timestamp())

        main.__exit__(None, None, None)

    def test_updates(self):
        main = queue.ExclusiveResourceProcessor(FAKE_ID)
        not_main = queue.ExclusiveResourceProcessor(FAKE_ID)

        main.queue_update(queue.ResourceUpdate(FAKE_ID, 0))
        not_main.queue_update(queue.ResourceUpdate(FAKE_ID, 0))

        for update in not_main.updates():
            raise Exception("Only the main should process a resource")

        self.assertEqual(2, len([i for i in main.updates()]))

    def test_hit_retry_limit(self):
        tries = 1
        rpqueue = queue.ResourceProcessingQueue()
        update = queue.ResourceUpdate(FAKE_ID, PRIORITY_RPC, tries=tries)
        rpqueue.add(update)
        self.assertFalse(update.hit_retry_limit())
        rpqueue.add(update)
        self.assertTrue(update.hit_retry_limit())
