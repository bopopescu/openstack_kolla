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

from neutron.objects.port.extensions import uplink_status_propagation
from neutron.tests.unit.objects import test_base as obj_test_base
from neutron.tests.unit import testlib_api


class UplinkStatusPropagationIfaceObjectTestCase(
    obj_test_base.BaseObjectIfaceTestCase):

    _test_class = uplink_status_propagation.PortUplinkStatusPropagation


class UplinkStatusPropagationDbObjectTestCase(
    obj_test_base.BaseDbObjectTestCase,
    testlib_api.SqlTestCase):

    _test_class = uplink_status_propagation.PortUplinkStatusPropagation

    def setUp(self):
        super(UplinkStatusPropagationDbObjectTestCase, self).setUp()
        self.update_obj_fields(
            {'port_id': lambda: self._create_test_port_id()})
