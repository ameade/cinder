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
from cinder.volume.drivers.netapp.client import base


LOG = logging.getLogger(__name__)


class Client(base.Client):

    def __init__(self, connection, volume_list):
        super(Client, self).__init__(connection)
        self.volume_list = volume_list

    def get_target_details(self):
        """Gets the target portal details."""
        iscsi_if_iter = netapp_api.NaElement('iscsi-portal-list-info')
        result = self.connection.invoke_successfully(iscsi_if_iter, True)
        tgt_list = []
        portal_list_entries = result.get_child_by_name(
            'iscsi-portal-list-entries')
        if portal_list_entries:
            portal_list = portal_list_entries.get_children()
            for iscsi_if in portal_list:
                d = dict()
                d['address'] = iscsi_if.get_child_content('ip-address')
                d['port'] = iscsi_if.get_child_content('ip-port')
                d['tpgroup-tag'] = iscsi_if.get_child_content('tpgroup-tag')
                tgt_list.append(d)
        return tgt_list

    def get_iscsi_service_details(self):
        """Returns iscsi iqn."""
        iscsi_service_iter = netapp_api.NaElement('iscsi-node-get-name')
        result = self.connection.invoke_successfully(iscsi_service_iter, True)
        return result.get_child_content('node-name')

    def get_lun_list(self):
        """Gets the list of luns on filer."""
        lun_list = []
        if self.volume_list:
            for vol in self.volume_list:
                try:
                    luns = self._get_vol_luns(vol)
                    if luns:
                        lun_list.extend(luns)
                except netapp_api.NaApiError:
                    LOG.warn(_("Error finding luns for volume %s."
                               " Verify volume exists.") % (vol))
        else:
            luns = self._get_vol_luns(None)
            lun_list.extend(luns)
        return lun_list

    def _get_vol_luns(self, vol_name):
        """Gets the luns for a volume."""
        api = netapp_api.NaElement('lun-list-info')
        if vol_name:
            api.add_new_child('volume-name', vol_name)
        result = self.connection.invoke_successfully(api, True)
        luns = result.get_child_by_name('luns')
        return luns.get_children()

    def get_igroup_by_initiator(self, initiator):
        """Get igroups by initiator."""
        igroup_list = netapp_api.NaElement('igroup-list-info')
        result = self.connection.invoke_successfully(igroup_list, True)
        igroups = []
        igs = result.get_child_by_name('initiator-groups')
        if igs:
            ig_infos = igs.get_children()
            if ig_infos:
                for info in ig_infos:
                    initiators = info.get_child_by_name('initiators')
                    init_infos = initiators.get_children()
                    if init_infos:
                        for init in init_infos:
                            if init.get_child_content('initiator-name')\
                                    == initiator:
                                d = dict()
                                d['initiator-group-os-type'] = \
                                    info.get_child_content(
                                        'initiator-group-os-type')
                                d['initiator-group-type'] = \
                                    info.get_child_content(
                                        'initiator-group-type')
                                d['initiator-group-name'] = \
                                    info.get_child_content(
                                        'initiator-group-name')
                                igroups.append(d)
        return igroups
