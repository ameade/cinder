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

    def get_target_details(self):
        """Gets the target portal details."""
        iscsi_if_iter = netapp_api.NaElement('iscsi-interface-get-iter')
        result = self.connection.invoke_successfully(iscsi_if_iter, True)
        tgt_list = []
        if result.get_child_content('num-records')\
                and int(result.get_child_content('num-records')) >= 1:
            attr_list = result.get_child_by_name('attributes-list')
            iscsi_if_list = attr_list.get_children()
            for iscsi_if in iscsi_if_list:
                d = dict()
                d['address'] = iscsi_if.get_child_content('ip-address')
                d['port'] = iscsi_if.get_child_content('ip-port')
                d['tpgroup-tag'] = iscsi_if.get_child_content('tpgroup-tag')
                d['interface-enabled'] = iscsi_if.get_child_content(
                    'is-interface-enabled')
                tgt_list.append(d)
        return tgt_list
