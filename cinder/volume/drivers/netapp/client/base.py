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

import sys


from cinder.openstack.common import log as logging
from cinder.volume.drivers.netapp import api as netapp_api


LOG = logging.getLogger(__name__)


class Client(object):

    def __init__(self, connection):
        self.connection = connection

    def get_ontapi_version(self):
        """Gets the supported ontapi version."""
        ontapi_version = netapp_api.NaElement('system-get-ontapi-version')
        res = self.connection.invoke_successfully(ontapi_version, False)
        major = res.get_child_content('major-version')
        minor = res.get_child_content('minor-version')
        return (major, minor)

    def create_lun(self, volume, lun, size, metadata, qos_policy_group=None):
        """Issues api request for creating lun on volume."""
        path = '/vol/%s/%s' % (volume, lun)
        lun_create = netapp_api.NaElement.create_node_with_children(
            'lun-create-by-size',
            **{'path': path, 'size': size,
                'ostype': metadata['OsType'],
                'space-reservation-enabled': metadata['SpaceReserved']})
        if qos_policy_group:
            lun_create.add_new_child('qos-policy-group', qos_policy_group)
        self.connection.invoke_successfully(lun_create, True)

    def destroy_lun(self, path, force=True):
        """Destroys the lun at the path."""
        lun_destroy = netapp_api.NaElement.create_node_with_children(
            'lun-destroy',
            **{'path': path})
        if force:
            lun_destroy.add_new_child('force', 'true')
        self.connection.invoke_successfully(lun_destroy, True)
        seg = path.split("/")
        LOG.debug(_("Destroyed LUN %s") % seg[-1])

    def map_lun(self, path, igroup_name, lun_id=None):
        """Maps lun to the initiator and returns lun id assigned."""
        lun_map = netapp_api.NaElement.create_node_with_children(
            'lun-map', **{'path': path,
                          'initiator-group': igroup_name})
        if lun_id:
            lun_map.add_new_child('lun-id', lun_id)
        try:
            result = self.connection.invoke_successfully(lun_map, True)
            return result.get_child_content('lun-id-assigned')
        except netapp_api.NaApiError as e:
            code = e.code
            message = e.message
            msg = _('Error mapping lun. Code :%(code)s, Message:%(message)s')
            msg_fmt = {'code': code, 'message': message}
            LOG.warn(msg % msg_fmt)
            raise

    def unmap_lun(self, path, igroup_name):
        """Unmaps a lun from given initiator."""
        lun_unmap = netapp_api.NaElement.create_node_with_children(
            'lun-unmap',
            **{'path': path, 'initiator-group': igroup_name})
        try:
            self.connection.invoke_successfully(lun_unmap, True)
        except netapp_api.NaApiError as e:
            msg = _("Error unmapping lun. Code :%(code)s,"
                    " Message:%(message)s")
            msg_fmt = {'code': e.code, 'message': e.message}
            exc_info = sys.exc_info()
            LOG.warn(msg % msg_fmt)
            # if the lun is already unmapped
            if e.code == '13115' or e.code == '9016':
                pass
            else:
                raise exc_info[0], exc_info[1], exc_info[2]
