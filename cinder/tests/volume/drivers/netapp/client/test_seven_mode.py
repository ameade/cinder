# Copyright (c) 2014 NetApp, Inc.
# All Rights Reserved.
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

from lxml import etree
import mock
import uuid

from cinder import test
from cinder.volume.drivers.netapp import api as netapp_api
from cinder.volume.drivers.netapp.client import seven_mode


class NetApp7modeClientTestCase(test.TestCase):

    def setUp(self):
        super(NetApp7modeClientTestCase, self).setUp()
        self.connection = mock.MagicMock()
        self.fake_volume = str(uuid.uuid4())
        self.client = seven_mode.Client(self.connection, [self.fake_volume])
        self.fake_lun = str(uuid.uuid4())
        self.fake_size = '1024'
        self.fake_metadata = {
            'OsType': 'linux',
            'SpaceReserved': 'true',
        }

    def tearDown(self):
        super(NetApp7modeClientTestCase, self).tearDown()

    def test_get_target_details_no_targets(self):
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <iscsi-portal-list-entries>
                           </iscsi-portal-list-entries>
                         </results>"""))
        self.connection.invoke_successfully.return_value = response

        target_list = self.client.get_target_details()

        self.assertEqual([], target_list)

    def test_get_target_details(self):
        expected_target = {
            "address": "127.0.0.1",
            "port": "1337",
            "tpgroup-tag": "7777",
        }
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <iscsi-portal-list-entries>
                              <iscsi-portal-list-entry-info>
                                <ip-address>%(address)s</ip-address>
                                <ip-port>%(port)s</ip-port>
                                <tpgroup-tag>%(tpgroup-tag)s</tpgroup-tag>
                              </iscsi-portal-list-entry-info>
                           </iscsi-portal-list-entries>
                          </results>""" % expected_target))
        self.connection.invoke_successfully.return_value = response

        target_list = self.client.get_target_details()

        self.assertEqual([expected_target], target_list)

    def test_get_iscsi_service_details_with_no_iscsi_service(self):
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                         </results>"""))
        self.connection.invoke_successfully.return_value = response

        iqn = self.client.get_iscsi_service_details()

        self.assertEqual(None, iqn)

    def test_get_iscsi_service_details(self):
        expected_iqn = 'iqn.1998-01.org.openstack.iscsi:name1'
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <node-name>%s</node-name>
                         </results>""" % expected_iqn))
        self.connection.invoke_successfully.return_value = response

        iqn = self.client.get_iscsi_service_details()

        self.assertEqual(expected_iqn, iqn)

    def test_get_lun_list(self):
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <luns>
                            <lun-info></lun-info>
                            <lun-info></lun-info>
                           </luns>
                          </results>"""))
        self.connection.invoke_successfully.return_value = response

        luns = self.client.get_lun_list()

        self.assertEqual(2, len(luns))

    def test_get_igroup_by_initiator_none_found(self):
        initiator = 'initiator'
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <initiator-groups>
                           </initiator-groups>
                         </results>"""))
        self.connection.invoke_successfully.return_value = response

        igroup = self.client.get_igroup_by_initiator(initiator)

        self.assertEqual([], igroup)

    def test_get_igroup_by_initiator(self):
        initiator = 'initiator'
        expected_igroup = {
            "initiator-group-os-type": None,
            "initiator-group-type": "1337",
            "initiator-group-name": "vserver",
        }
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <initiator-groups>
                             <initiator-group-info>
                               <initiators>
                                 <initiator-info>
                                   <initiator-name>initiator</initiator-name>
                                 </initiator-info>
                               </initiators>
    <initiator-group-type>%(initiator-group-type)s</initiator-group-type>
    <initiator-group-name>%(initiator-group-name)s</initiator-group-name>
                             </initiator-group-info>
                           </initiator-groups>
                         </results>""" % expected_igroup))
        self.connection.invoke_successfully.return_value = response

        igroup = self.client.get_igroup_by_initiator(initiator)

        self.assertEqual([expected_igroup], igroup)

    def test_clone_lun(self):
        fake_clone_start = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <clone-id>
                             <clone-id-info>
                               <clone-op-id>1337</clone-op-id>
                               <volume-uuid>volume-uuid</volume-uuid>
                             </clone-id-info>
                           </clone-id>
                         </results>"""))
        fake_clone_status = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <status>
                             <ops-info>
                               <clone-state>completed</clone-state>
                             </ops-info>
                           </status>
                         </results>"""))

        self.connection.invoke_successfully.side_effect = [fake_clone_start,
                                                           fake_clone_status]

        self.client.clone_lun('path', 'new_path', 'fakeLUN', 'newFakeLUN')
        self.assertEqual(2, self.connection.invoke_successfully.call_count)

    def test_clone_lun_api_error(self):
        fake_clone_start = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <clone-id>
                             <clone-id-info>
                               <clone-op-id>1337</clone-op-id>
                               <volume-uuid>volume-uuid</volume-uuid>
                             </clone-id-info>
                           </clone-id>
                         </results>"""))
        fake_clone_status = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <status>
                             <ops-info>
                               <clone-state>error</clone-state>
                             </ops-info>
                           </status>
                         </results>"""))

        self.connection.invoke_successfully.side_effect = [fake_clone_start,
                                                           fake_clone_status]

        self.assertRaises(netapp_api.NaApiError, self.client.clone_lun,
                          'path', 'new_path', 'fakeLUN', 'newFakeLUN')

    def test_clone_lun_multiple_zapi_calls(self):
        # Max block-ranges per call = 32, max blocks per range = 2^24
        # Force 2 calls
        bc = 2 ** 24 * 32 * 2
        fake_clone_start = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <clone-id>
                             <clone-id-info>
                               <clone-op-id>1337</clone-op-id>
                               <volume-uuid>volume-uuid</volume-uuid>
                             </clone-id-info>
                           </clone-id>
                         </results>"""))
        fake_clone_status = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <status>
                             <ops-info>
                               <clone-state>completed</clone-state>
                             </ops-info>
                           </status>
                         </results>"""))

        self.connection.invoke_successfully.side_effect = [fake_clone_start,
                                                           fake_clone_status,
                                                           fake_clone_start,
                                                           fake_clone_status]

        self.client.clone_lun('path', 'new_path', 'fakeLUN', 'newFakeLUN',
                              block_count=bc)

        self.assertEqual(4, self.connection.invoke_successfully.call_count)

    def test_clone_lun_wait_for_clone_to_finish(self):
        # Max block-ranges per call = 32, max blocks per range = 2^24
        # Force 2 calls
        bc = 2 ** 24 * 32 * 2
        fake_clone_start = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <clone-id>
                             <clone-id-info>
                               <clone-op-id>1337</clone-op-id>
                               <volume-uuid>volume-uuid</volume-uuid>
                             </clone-id-info>
                           </clone-id>
                         </results>"""))
        fake_clone_status = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <status>
                             <ops-info>
                               <clone-state>running</clone-state>
                             </ops-info>
                           </status>
                         </results>"""))
        fake_clone_status_completed = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                           <status>
                             <ops-info>
                               <clone-state>completed</clone-state>
                             </ops-info>
                           </status>
                         </results>"""))

        fake_responses = [fake_clone_start,
                          fake_clone_status,
                          fake_clone_status_completed,
                          fake_clone_start,
                          fake_clone_status_completed]
        self.connection.invoke_successfully.side_effect = fake_responses

        with mock.patch('time.sleep') as mock_sleep:
            self.client.clone_lun('path', 'new_path', 'fakeLUN',
                                  'newFakeLUN', block_count=bc)

            mock_sleep.assert_called_once_with(1)
            self.assertEqual(5, self.connection.invoke_successfully.call_count)
