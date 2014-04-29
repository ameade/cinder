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
from cinder.volume.drivers.netapp.client import base


class NetAppBaseClientTestCase(test.TestCase):

    def setUp(self):
        super(NetAppBaseClientTestCase, self).setUp()
        self.connection = mock.MagicMock()
        self.client = base.Client(self.connection)
        self.fake_volume = str(uuid.uuid4())
        self.fake_lun = str(uuid.uuid4())
        self.fake_size = '1024'
        self.fake_metadata = {
            'OsType': 'linux',
            'SpaceReserved': 'true',
        }

    def tearDown(self):
        super(NetAppBaseClientTestCase, self).tearDown()

    def test_get_ontapi_version(self):
        version_response = netapp_api.NaElement(
            etree.XML("""<results status="passed">
                            <major-version>1</major-version>
                            <minor-version>19</minor-version>
                          </results>"""))
        self.connection.invoke_successfully.return_value = version_response

        major, minor = self.client.get_ontapi_version()

        self.assertEqual('1', major)
        self.assertEqual('19', minor)

    def test_create_lun(self):
        expected_path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            self.client.create_lun(self.fake_volume,
                                   self.fake_lun,
                                   self.fake_size,
                                   self.fake_metadata)

            mock_create_node.assert_called_once_with(
                'lun-create-by-size',
                **{'path': expected_path,
                   'size': self.fake_size,
                   'ostype': self.fake_metadata['OsType'],
                   'space-reservation-enabled':
                   self.fake_metadata['SpaceReserved']})
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)

    def test_create_lun_with_qos_policy_group(self):
        expected_path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        expected_qos_group = 'qos_1'
        mock_request = mock.Mock()

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               return_value=mock_request
                               ) as mock_create_node:
            self.client.create_lun(self.fake_volume,
                                   self.fake_lun,
                                   self.fake_size,
                                   self.fake_metadata,
                                   qos_policy_group=expected_qos_group)

            mock_create_node.assert_called_once_with(
                'lun-create-by-size',
                **{'path': expected_path, 'size': self.fake_size,
                    'ostype': self.fake_metadata['OsType'],
                    'space-reservation-enabled':
                    self.fake_metadata['SpaceReserved']})
            mock_request.add_new_child.assert_called_once_with(
                'qos-policy-group', expected_qos_group)
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)

    def test_destroy_lun(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            self.client.destroy_lun(path)

            mock_create_node.assert_called_once_with(
                'lun-destroy',
                **{'path': path})
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)

    def test_destroy_lun_force(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        mock_request = mock.Mock()

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               return_value=mock_request
                               ) as mock_create_node:
            self.client.destroy_lun(path)

            mock_create_node.assert_called_once_with('lun-destroy',
                                                     **{'path': path})
            mock_request.add_new_child.assert_called_once_with('force', 'true')
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)

    def test_map_lun(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        igroup = 'igroup'
        expected_lun_id = 'my_lun'
        mock_response = mock.Mock()
        self.connection.invoke_successfully.return_value = mock_response
        mock_response.get_child_content.return_value = expected_lun_id

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            actual_lun_id = self.client.map_lun(path, igroup)

            mock_create_node.assert_called_once_with(
                'lun-map',
                **{'path': path, 'initiator-group': igroup})
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)
            self.assertEqual(expected_lun_id, actual_lun_id)

    def test_map_lun_with_lun_id(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        igroup = 'igroup'
        expected_lun_id = 'my_lun'
        mock_response = mock.Mock()
        self.connection.invoke_successfully.return_value = mock_response
        mock_response.get_child_content.return_value = expected_lun_id

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            actual_lun_id = self.client.map_lun(path, igroup,
                                                lun_id=expected_lun_id)

            mock_create_node.assert_called_once_with(
                'lun-map',
                **{'path': path, 'initiator-group': igroup})
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)
            self.assertEqual(expected_lun_id, actual_lun_id)

    def test_unmap_lun(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        igroup = 'igroup'
        mock_response = mock.Mock()
        self.connection.invoke_successfully.return_value = mock_response

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            self.client.unmap_lun(path, igroup)

            mock_create_node.assert_called_once_with(
                'lun-unmap',
                **{'path': path, 'initiator-group': igroup})
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)

    def test_unmap_lun_with_api_error(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        igroup = 'igroup'
        self.connection.invoke_successfully.side_effect =\
            netapp_api.NaApiError()

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            self.assertRaises(netapp_api.NaApiError, self.client.unmap_lun,
                              path, igroup)

            mock_create_node.assert_called_once_with(
                'lun-unmap',
                **{'path': path, 'initiator-group': igroup})

    def test_unmap_lun_already_unmapped(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        igroup = 'igroup'
        EINVALIDINPUTERROR = '13115'
        self.connection.invoke_successfully.side_effect =\
            netapp_api.NaApiError(code=EINVALIDINPUTERROR)

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            self.client.unmap_lun(path, igroup)

            mock_create_node.assert_called_once_with(
                'lun-unmap',
                **{'path': path, 'initiator-group': igroup})
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)

    def test_unmap_lun_lun_not_mapped_in_group(self):
        path = '/vol/%s/%s' % (self.fake_volume, self.fake_lun)
        igroup = 'igroup'
        EVDISK_ERROR_NO_SUCH_LUNMAP = '9016'
        self.connection.invoke_successfully.side_effect =\
            netapp_api.NaApiError(code=EVDISK_ERROR_NO_SUCH_LUNMAP)

        with mock.patch.object(netapp_api.NaElement,
                               'create_node_with_children',
                               ) as mock_create_node:
            self.client.unmap_lun(path, igroup)

            mock_create_node.assert_called_once_with(
                'lun-unmap',
                **{'path': path, 'initiator-group': igroup})
            self.connection.invoke_successfully.assert_called_once_with(
                mock.ANY, True)
