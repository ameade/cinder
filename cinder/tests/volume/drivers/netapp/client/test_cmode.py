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
from cinder.volume.drivers.netapp.client import cmode


class NetAppCmodeClientTestCase(test.TestCase):

    def setUp(self):
        super(NetAppCmodeClientTestCase, self).setUp()
        self.connection = mock.MagicMock()
        self.client = cmode.Client(self.connection, 'fake_vserver')
        self.fake_volume = str(uuid.uuid4())
        self.fake_lun = str(uuid.uuid4())
        self.fake_size = '1024'
        self.fake_metadata = {
            'OsType': 'linux',
            'SpaceReserved': 'true',
        }

    def tearDown(self):
        super(NetAppCmodeClientTestCase, self).tearDown()

    def test_get_target_details_no_targets(self):
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list></attributes-list>
                          </results>"""))
        self.connection.invoke_successfully.return_value = response

        target_list = self.client.get_target_details()

        self.assertEqual([], target_list)

    def test_get_target_details(self):
        expected_target = {
            "address": "127.0.0.1",
            "port": "1337",
            "interface-enabled": "true",
            "tpgroup-tag": "7777",
        }
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list>
                              <iscsi-interface-list-entry-info>
                                <ip-address>%(address)s</ip-address>
                                <ip-port>%(port)s</ip-port>
            <is-interface-enabled>%(interface-enabled)s</is-interface-enabled>
                                <tpgroup-tag>%(tpgroup-tag)s</tpgroup-tag>
                              </iscsi-interface-list-entry-info>
                            </attributes-list>
                          </results>""" % expected_target))
        self.connection.invoke_successfully.return_value = response

        target_list = self.client.get_target_details()

        self.assertEqual([expected_target], target_list)

    def test_get_iscsi_service_details_with_no_iscsi_service(self):
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>0</num-records>
                          </results>"""))
        self.connection.invoke_successfully.return_value = response

        iqn = self.client.get_iscsi_service_details()

        self.assertEqual(None, iqn)

    def test_get_iscsi_service_details(self):
        expected_iqn = 'iqn.1998-01.org.openstack.iscsi:name1'
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list>
                              <iscsi-service-info>
                                <node-name>%s</node-name>
                              </iscsi-service-info>
                            </attributes-list>
                          </results>""" % expected_iqn))
        self.connection.invoke_successfully.return_value = response

        iqn = self.client.get_iscsi_service_details()

        self.assertEqual(expected_iqn, iqn)

    def test_get_lun_list(self):
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>2</num-records>
                            <attributes-list>
                              <lun-info>
                              </lun-info>
                              <lun-info>
                              </lun-info>
                            </attributes-list>
                          </results>"""))
        self.connection.invoke_successfully.return_value = response

        luns = self.client.get_lun_list()

        self.assertEqual(2, len(luns))

    def test_get_lun_list_with_multiple_pages(self):
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>2</num-records>
                            <attributes-list>
                              <lun-info> </lun-info>
                              <lun-info> </lun-info>
                            </attributes-list>
                            <next-tag>fake-next</next-tag>
                          </results>"""))
        response_2 = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>2</num-records>
                            <attributes-list>
                              <lun-info> </lun-info>
                              <lun-info> </lun-info>
                            </attributes-list>
                          </results>"""))
        self.connection.invoke_successfully.side_effect = [response,
                                                           response_2]

        luns = self.client.get_lun_list()

        self.assertEqual(4, len(luns))

    def test_get_lun_map_no_luns_mapped(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>0</num-records>
                            <attributes-list></attributes-list>
                          </results>"""))
        self.connection.invoke_successfully.return_value = response

        lun_map = self.client.get_lun_map(path)

        self.assertEqual([], lun_map)

    def test_get_lun_map(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        expected_lun_map = {
            "initiator-group": "igroup",
            "lun-id": "1337",
            "vserver": "vserver",
        }
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list>
                              <lun-map-info>
                                <lun-id>%(lun-id)s</lun-id>
                        <initiator-group>%(initiator-group)s</initiator-group>
                                <vserver>%(vserver)s</vserver>
                              </lun-map-info>
                            </attributes-list>
                          </results>""" % expected_lun_map))
        self.connection.invoke_successfully.return_value = response

        lun_map = self.client.get_lun_map(path)

        self.assertEqual([expected_lun_map], lun_map)

    def test_get_lun_map_multiple_pages(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        expected_lun_map = {
            "initiator-group": "igroup",
            "lun-id": "1337",
            "vserver": "vserver",
        }
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list>
                              <lun-map-info>
                                <lun-id>%(lun-id)s</lun-id>
                        <initiator-group>%(initiator-group)s</initiator-group>
                                <vserver>%(vserver)s</vserver>
                              </lun-map-info>
                            </attributes-list>
                            <next-tag>blah</next-tag>
                          </results>""" % expected_lun_map))
        response_2 = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list>
                              <lun-map-info>
                                <lun-id>%(lun-id)s</lun-id>
                        <initiator-group>%(initiator-group)s</initiator-group>
                                <vserver>%(vserver)s</vserver>
                              </lun-map-info>
                            </attributes-list>
                          </results>""" % expected_lun_map))
        self.connection.invoke_successfully.side_effect = [response,
                                                           response_2]

        lun_map = self.client.get_lun_map(path)

        self.assertEqual([expected_lun_map, expected_lun_map], lun_map)

    def test_get_igroup_by_initiator_none_found(self):
        initiator = 'initiator'
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>0</num-records>
                            <attributes-list></attributes-list>
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
                            <num-records>1</num-records>
                            <attributes-list>
                              <initiator-group-info>
    <initiator-group-type>%(initiator-group-type)s</initiator-group-type>
    <initiator-group-name>%(initiator-group-name)s</initiator-group-name>
                              </initiator-group-info>
                            </attributes-list>
                          </results>""" % expected_igroup))
        self.connection.invoke_successfully.return_value = response

        igroup = self.client.get_igroup_by_initiator(initiator)

        self.assertEqual([expected_igroup], igroup)

    def test_get_igroup_by_initiator_multiple_pages(self):
        initiator = 'initiator'
        expected_igroup = {
            "initiator-group-os-type": None,
            "initiator-group-type": "1337",
            "initiator-group-name": "vserver",
        }
        response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list>
                              <initiator-group-info>
    <initiator-group-type>%(initiator-group-type)s</initiator-group-type>
    <initiator-group-name>%(initiator-group-name)s</initiator-group-name>
                              </initiator-group-info>
                            </attributes-list>
                            <next-tag>blah</next-tag>
                          </results>""" % expected_igroup))
        response_2 = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <num-records>1</num-records>
                            <attributes-list>
                              <initiator-group-info>
    <initiator-group-type>%(initiator-group-type)s</initiator-group-type>
    <initiator-group-name>%(initiator-group-name)s</initiator-group-name>
                              </initiator-group-info>
                            </attributes-list>
                          </results>""" % expected_igroup))
        self.connection.invoke_successfully.side_effect = [response,
                                                           response_2]

        igroup = self.client.get_igroup_by_initiator(initiator)

        self.assertEqual([expected_igroup, expected_igroup], igroup)
