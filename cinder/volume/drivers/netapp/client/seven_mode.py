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


import math
import time


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

    def clone_lun(self, path, clone_path, name, new_name,
                  space_reserved='true', src_block=0,
                  dest_block=0, block_count=0):
        # zAPI can only handle 2^24 blocks per range
        bc_limit = 2 ** 24  # 8GB
        # zAPI can only handle 32 block ranges per call
        br_limit = 32
        z_limit = br_limit * bc_limit  # 256 GB
        z_calls = int(math.ceil(block_count / float(z_limit)))
        zbc = block_count
        if z_calls == 0:
            z_calls = 1
        for call in range(0, z_calls):
            if zbc > z_limit:
                block_count = z_limit
                zbc -= z_limit
            else:
                block_count = zbc
            clone_start = netapp_api.NaElement.create_node_with_children(
                'clone-start', **{'source-path': path,
                                  'destination-path': clone_path,
                                  'no-snap': 'true'})
            if block_count > 0:
                block_ranges = netapp_api.NaElement("block-ranges")
                # zAPI can only handle 2^24 block ranges
                bc_limit = 2 ** 24  # 8GB
                segments = int(math.ceil(block_count / float(bc_limit)))
                bc = block_count
                for segment in range(0, segments):
                    if bc > bc_limit:
                        block_count = bc_limit
                        bc -= bc_limit
                    else:
                        block_count = bc
                    block_range =\
                        netapp_api.NaElement.create_node_with_children(
                            'block-range',
                            **{'source-block-number': str(src_block),
                                'destination-block-number': str(dest_block),
                                'block-count': str(block_count)})
                    block_ranges.add_child_elem(block_range)
                    src_block += int(block_count)
                    dest_block += int(block_count)
                clone_start.add_child_elem(block_ranges)
            result = self.connection.invoke_successfully(clone_start, True)
            clone_id_el = result.get_child_by_name('clone-id')
            cl_id_info = clone_id_el.get_child_by_name('clone-id-info')
            vol_uuid = cl_id_info.get_child_content('volume-uuid')
            clone_id = cl_id_info.get_child_content('clone-op-id')
            if vol_uuid:
                self._check_clone_status(clone_id, vol_uuid, name, new_name)

    def _check_clone_status(self, clone_id, vol_uuid, name, new_name):
        """Checks for the job till completed."""
        clone_status = netapp_api.NaElement('clone-list-status')
        cl_id = netapp_api.NaElement('clone-id')
        clone_status.add_child_elem(cl_id)
        cl_id.add_node_with_children('clone-id-info',
                                     **{'clone-op-id': clone_id,
                                     'volume-uuid': vol_uuid})
        running = True
        clone_ops_info = None
        while running:
            result = self.connection.invoke_successfully(clone_status, True)
            status = result.get_child_by_name('status')
            ops_info = status.get_children()
            if ops_info:
                for info in ops_info:
                    if info.get_child_content('clone-state') == 'running':
                        time.sleep(1)
                        break
                    else:
                        running = False
                        clone_ops_info = info
                        break
        else:
            if clone_ops_info:
                fmt = {'name': name, 'new_name': new_name}
                if clone_ops_info.get_child_content('clone-state')\
                        == 'completed':
                    LOG.debug(_("Clone operation with src %(name)s"
                                " and dest %(new_name)s completed") % fmt)
                else:
                    LOG.debug(_("Clone operation with src %(name)s"
                                " and dest %(new_name)s failed") % fmt)
                    raise netapp_api.NaApiError(
                        clone_ops_info.get_child_content('error'),
                        clone_ops_info.get_child_content('reason'))

    def get_lun_by_args(self, **args):
        """Retrieves luns with specified args."""
        lun_info = netapp_api.NaElement.create_node_with_children(
            'lun-list-info', **args)
        result = self.connection.invoke_successfully(lun_info, True)
        luns = result.get_child_by_name('luns')
        return luns.get_children()
