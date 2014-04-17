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
