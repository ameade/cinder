# Copyright (c) 2012 NetApp, Inc.
# Copyright (c) 2012 OpenStack Foundation
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
"""
Volume driver for NetApp iSCSI storage systems.

This driver requires NetApp Clustered Data ONTAP or 7-mode
storage systems with installed iSCSI licenses.
"""

import copy
import sys
import uuid

from cinder import exception
from cinder.openstack.common import excutils
from cinder.openstack.common import log as logging
from cinder.openstack.common import timeutils
from cinder import units
from cinder import utils
from cinder.volume import driver
from cinder.volume.drivers.netapp.api import NaApiError
from cinder.volume.drivers.netapp.api import NaElement
from cinder.volume.drivers.netapp.api import NaServer
from cinder.volume.drivers.netapp.client import cmode
from cinder.volume.drivers.netapp.client import seven_mode
from cinder.volume.drivers.netapp.options import netapp_7mode_opts
from cinder.volume.drivers.netapp.options import netapp_basicauth_opts
from cinder.volume.drivers.netapp.options import netapp_cluster_opts
from cinder.volume.drivers.netapp.options import netapp_connection_opts
from cinder.volume.drivers.netapp.options import netapp_provisioning_opts
from cinder.volume.drivers.netapp.options import netapp_transport_opts
from cinder.volume.drivers.netapp import ssc_utils
from cinder.volume.drivers.netapp.utils import get_volume_extra_specs
from cinder.volume.drivers.netapp.utils import provide_ems
from cinder.volume.drivers.netapp.utils import set_safe_attr
from cinder.volume.drivers.netapp.utils import validate_instantiation


LOG = logging.getLogger(__name__)


class NetAppLun(object):
    """Represents a LUN on NetApp storage."""

    def __init__(self, handle, name, size, metadata_dict):
        self.handle = handle
        self.name = name
        self.size = size
        self.metadata = metadata_dict or {}

    def get_metadata_property(self, prop):
        """Get the metadata property of a LUN."""
        if prop in self.metadata:
            return self.metadata[prop]
        name = self.name
        msg = _("No metadata property %(prop)s defined for the"
                " LUN %(name)s")
        msg_fmt = {'prop': prop, 'name': name}
        LOG.debug(msg % msg_fmt)

    def __str__(self, *args, **kwargs):
        return 'NetApp Lun[handle:%s, name:%s, size:%s, metadata:%s]'\
               % (self.handle, self.name, self.size, self.metadata)


class NetAppDirectISCSIDriver(driver.ISCSIDriver):
    """NetApp Direct iSCSI volume driver."""

    VERSION = "1.0.0"

    IGROUP_PREFIX = 'openstack-'
    required_flags = ['netapp_transport_type', 'netapp_login',
                      'netapp_password', 'netapp_server_hostname',
                      'netapp_server_port']

    def __init__(self, *args, **kwargs):
        super(NetAppDirectISCSIDriver, self).__init__(*args, **kwargs)
        validate_instantiation(**kwargs)
        self.configuration.append_config_values(netapp_connection_opts)
        self.configuration.append_config_values(netapp_basicauth_opts)
        self.configuration.append_config_values(netapp_transport_opts)
        self.configuration.append_config_values(netapp_provisioning_opts)
        self.lun_table = {}

    def _create_client(self, **kwargs):
        """Instantiate a client for NetApp server.

        This method creates NetApp server client for api communication.
        """

        host_filer = kwargs['hostname']
        LOG.debug(_('Using NetApp filer: %s') % host_filer)
        self.client = NaServer(host=host_filer,
                               server_type=NaServer.SERVER_TYPE_FILER,
                               transport_type=kwargs['transport_type'],
                               style=NaServer.STYLE_LOGIN_PASSWORD,
                               username=kwargs['login'],
                               password=kwargs['password'])

    def _do_custom_setup(self):
        """Does custom setup depending on the type of filer."""
        raise NotImplementedError()

    def _check_flags(self):
        """Ensure that the flags we care about are set."""
        required_flags = self.required_flags
        for flag in required_flags:
            if not getattr(self.configuration, flag, None):
                msg = _('%s is not set') % flag
                raise exception.InvalidInput(reason=msg)

    def do_setup(self, context):
        """Setup the NetApp Volume driver.

        Called one time by the manager after the driver is loaded.
        Validate the flags we care about and setup NetApp
        client.
        """

        self._check_flags()
        self._create_client(
            transport_type=self.configuration.netapp_transport_type,
            login=self.configuration.netapp_login,
            password=self.configuration.netapp_password,
            hostname=self.configuration.netapp_server_hostname,
            port=self.configuration.netapp_server_port)
        self._do_custom_setup()

    def check_for_setup_error(self):
        """Check that the driver is working and can communicate.

        Discovers the LUNs on the NetApp server.
        """

        self.lun_table = {}
        lun_list = self.nclient.get_lun_list()
        self._extract_and_populate_luns(lun_list)
        LOG.debug(_("Success getting LUN list from server"))

    def create_volume(self, volume):
        """Driver entry point for creating a new volume."""
        default_size = '104857600'  # 100 MB
        gigabytes = 1073741824L  # 2^30
        name = volume['name']
        if int(volume['size']) == 0:
            size = default_size
        else:
            size = str(int(volume['size']) * gigabytes)
        metadata = {}
        metadata['OsType'] = 'linux'
        metadata['SpaceReserved'] = 'true'
        extra_specs = get_volume_extra_specs(volume)
        self._create_lun_on_eligible_vol(name, size, metadata, extra_specs)
        LOG.debug(_("Created LUN with name %s") % name)
        handle = self._create_lun_handle(metadata)
        self._add_lun_to_table(NetAppLun(handle, name, size, metadata))

    def delete_volume(self, volume):
        """Driver entry point for destroying existing volumes."""
        name = volume['name']
        metadata = self._get_lun_attr(name, 'metadata')
        if not metadata:
            msg = _("No entry in LUN table for volume/snapshot %(name)s.")
            msg_fmt = {'name': name}
            LOG.warn(msg % msg_fmt)
            return
        self.nclient.destroy_lun(metadata['Path'])
        self.lun_table.pop(name)

    def ensure_export(self, context, volume):
        """Driver entry point to get the export info for an existing volume."""
        handle = self._get_lun_attr(volume['name'], 'handle')
        return {'provider_location': handle}

    def create_export(self, context, volume):
        """Driver entry point to get the export info for a new volume."""
        handle = self._get_lun_attr(volume['name'], 'handle')
        return {'provider_location': handle}

    def remove_export(self, context, volume):
        """Driver entry point to remove an export for a volume.

        Since exporting is idempotent in this driver, we have nothing
        to do for unexporting.
        """

        pass

    def initialize_connection(self, volume, connector):
        """Driver entry point to attach a volume to an instance.

        Do the LUN masking on the storage system so the initiator can access
        the LUN on the target. Also return the iSCSI properties so the
        initiator can find the LUN. This implementation does not call
        _get_iscsi_properties() to get the properties because cannot store the
        LUN number in the database. We only find out what the LUN number will
        be during this method call so we construct the properties dictionary
        ourselves.
        """

        initiator_name = connector['initiator']
        name = volume['name']
        lun_id = self._map_lun(name, initiator_name, 'iscsi', None)
        msg = _("Mapped LUN %(name)s to the initiator %(initiator_name)s")
        msg_fmt = {'name': name, 'initiator_name': initiator_name}
        LOG.debug(msg % msg_fmt)
        iqn = self.nclient.get_iscsi_service_details()
        target_details_list = self.nclient.get_target_details()
        msg = _("Successfully fetched target details for LUN %(name)s and "
                "initiator %(initiator_name)s")
        msg_fmt = {'name': name, 'initiator_name': initiator_name}
        LOG.debug(msg % msg_fmt)

        if not target_details_list:
            msg = _('Failed to get LUN target details for the LUN %s')
            raise exception.VolumeBackendAPIException(data=msg % name)
        target_details = None
        for tgt_detail in target_details_list:
            if tgt_detail.get('interface-enabled', 'true') == 'true':
                target_details = tgt_detail
                break
        if not target_details:
            target_details = target_details_list[0]

        if not target_details['address'] and target_details['port']:
            msg = _('Failed to get target portal for the LUN %s')
            raise exception.VolumeBackendAPIException(data=msg % name)
        if not iqn:
            msg = _('Failed to get target IQN for the LUN %s')
            raise exception.VolumeBackendAPIException(data=msg % name)

        properties = {}
        properties['target_discovered'] = False
        (address, port) = (target_details['address'], target_details['port'])
        properties['target_portal'] = '%s:%s' % (address, port)
        properties['target_iqn'] = iqn
        properties['target_lun'] = lun_id
        properties['volume_id'] = volume['id']

        auth = volume['provider_auth']
        if auth:
            (auth_method, auth_username, auth_secret) = auth.split()
            properties['auth_method'] = auth_method
            properties['auth_username'] = auth_username
            properties['auth_password'] = auth_secret

        return {
            'driver_volume_type': 'iscsi',
            'data': properties,
        }

    def create_snapshot(self, snapshot):
        """Driver entry point for creating a snapshot.

        This driver implements snapshots by using efficient single-file
        (LUN) cloning.
        """

        vol_name = snapshot['volume_name']
        snapshot_name = snapshot['name']
        lun = self.lun_table[vol_name]
        self._clone_lun(lun.name, snapshot_name, 'false')

    def delete_snapshot(self, snapshot):
        """Driver entry point for deleting a snapshot."""
        self.delete_volume(snapshot)
        LOG.debug(_("Snapshot %s deletion successful") % snapshot['name'])

    def create_volume_from_snapshot(self, volume, snapshot):
        """Driver entry point for creating a new volume from a snapshot.

        Many would call this "cloning" and in fact we use cloning to implement
        this feature.
        """

        vol_size = volume['size']
        snap_size = snapshot['volume_size']
        snapshot_name = snapshot['name']
        new_name = volume['name']
        self._clone_lun(snapshot_name, new_name, 'true')
        if vol_size != snap_size:
            try:
                self.extend_volume(volume, volume['size'])
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.error(
                        _("Resizing %s failed. Cleaning volume."), new_name)
                    self.delete_volume(volume)

    def terminate_connection(self, volume, connector, **kwargs):
        """Driver entry point to unattach a volume from an instance.

        Unmask the LUN on the storage system so the given initiator can no
        longer access it.
        """

        initiator_name = connector['initiator']
        name = volume['name']
        metadata = self._get_lun_attr(name, 'metadata')
        path = metadata['Path']
        self._unmap_lun(path, initiator_name)
        msg = _("Unmapped LUN %(name)s from the initiator "
                "%(initiator_name)s")
        msg_fmt = {'name': name, 'initiator_name': initiator_name}
        LOG.debug(msg % msg_fmt)

    def _create_lun_on_eligible_vol(self, name, size, metadata,
                                    extra_specs=None):
        """Creates an actual lun on filer."""
        raise NotImplementedError()

    def _create_lun_handle(self, metadata):
        """Returns lun handle based on filer type."""
        raise NotImplementedError()

    def _extract_and_populate_luns(self, api_luns):
        """Extracts the luns from api.

        Populates in the lun table.
        """

        for lun in api_luns:
            meta_dict = self._create_lun_meta(lun)
            path = lun.get_child_content('path')
            (rest, splitter, name) = path.rpartition('/')
            handle = self._create_lun_handle(meta_dict)
            size = lun.get_child_content('size')
            discovered_lun = NetAppLun(handle, name,
                                       size, meta_dict)
            self._add_lun_to_table(discovered_lun)

    def _is_naelement(self, elem):
        """Checks if element is NetApp element."""
        if not isinstance(elem, NaElement):
            raise ValueError('Expects NaElement')

    def _map_lun(self, name, initiator, initiator_type='iscsi', lun_id=None):
        """Maps lun to the initiator and returns lun id assigned."""
        metadata = self._get_lun_attr(name, 'metadata')
        os = metadata['OsType']
        path = metadata['Path']
        if self._check_allowed_os(os):
            os = os
        else:
            os = 'default'
        igroup_name = self._get_or_create_igroup(initiator,
                                                 initiator_type, os)
        try:
            return self.nclient.map_lun(path, igroup_name, lun_id=lun_id)
        except NaApiError:
            exc_info = sys.exc_info()
            (igroup, lun_id) = self._find_mapped_lun_igroup(path, initiator)
            if lun_id is not None:
                return lun_id
            else:
                raise exc_info[0], exc_info[1], exc_info[2]

    def _unmap_lun(self, path, initiator):
        """Unmaps a lun from given initiator."""
        (igroup_name, lun_id) = self._find_mapped_lun_igroup(path, initiator)
        self.nclient.unmap_lun(path, igroup_name)

    def _find_mapped_lun_igroup(self, path, initiator, os=None):
        """Find the igroup for mapped lun with initiator."""
        raise NotImplementedError()

    def _get_or_create_igroup(self, initiator, initiator_type='iscsi',
                              os='default'):
        """Checks for an igroup for an initiator.

        Creates igroup if not found.
        """

        igroups = self.nclient.get_igroup_by_initiator(initiator=initiator)
        igroup_name = None
        for igroup in igroups:
            if igroup['initiator-group-os-type'] == os:
                if igroup['initiator-group-type'] == initiator_type or \
                        igroup['initiator-group-type'] == 'mixed':
                    if igroup['initiator-group-name'].startswith(
                            self.IGROUP_PREFIX):
                        igroup_name = igroup['initiator-group-name']
                        break
        if not igroup_name:
            igroup_name = self.IGROUP_PREFIX + str(uuid.uuid4())
            self.nclient.create_igroup(igroup_name, initiator_type, os)
            self.nclient.add_igroup_initiator(igroup_name, initiator)
        return igroup_name

    def _check_allowed_os(self, os):
        """Checks if the os type supplied is NetApp supported."""
        if os in ['linux', 'aix', 'hpux', 'windows', 'solaris',
                  'netware', 'vmware', 'openvms', 'xen', 'hyper_v']:
            return True
        else:
            return False

    def _add_lun_to_table(self, lun):
        """Adds LUN to cache table."""
        if not isinstance(lun, NetAppLun):
            msg = _("Object is not a NetApp LUN.")
            raise exception.VolumeBackendAPIException(data=msg)
        self.lun_table[lun.name] = lun

    def _get_lun_from_table(self, name):
        """Gets LUN from cache table.

        Refreshes cache if lun not found in cache.
        """
        lun = self.lun_table.get(name)
        if lun is None:
            lun_list = self.nclient.get_lun_list()
            self._extract_and_populate_luns(lun_list)
            lun = self.lun_table.get(name)
            if lun is None:
                raise exception.VolumeNotFound(volume_id=name)
        return lun

    def _clone_lun(self, name, new_name, space_reserved='true',
                   src_block=0, dest_block=0, block_count=0):
        """Clone LUN with the given name to the new name."""
        raise NotImplementedError()

    def _get_lun_by_args(self, **args):
        """Retrieves luns with specified args."""
        raise NotImplementedError()

    def _get_lun_attr(self, name, attr):
        """Get the lun attribute if found else None."""
        try:
            attr = getattr(self._get_lun_from_table(name), attr)
            return attr
        except exception.VolumeNotFound as e:
            LOG.error(_("Message: %s"), e.msg)
        except Exception as e:
            LOG.error(_("Error getting lun attribute. Exception: %s"),
                      e.__str__())
        return None

    def _create_lun_meta(self, lun):
        raise NotImplementedError()

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        vol_size = volume['size']
        src_vol = self.lun_table[src_vref['name']]
        src_vol_size = src_vref['size']
        new_name = volume['name']
        self._clone_lun(src_vol.name, new_name, 'true')
        if vol_size != src_vol_size:
            try:
                self.extend_volume(volume, volume['size'])
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.error(
                        _("Resizing %s failed. Cleaning volume."), new_name)
                    self.delete_volume(volume)

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """

        if refresh:
            self._update_volume_stats()

        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""
        raise NotImplementedError()

    def extend_volume(self, volume, new_size):
        """Extend an existing volume to the new size."""
        name = volume['name']
        path = self.lun_table[name].metadata['Path']
        curr_size_bytes = str(self.lun_table[name].size)
        new_size_bytes = str(int(new_size) * units.GiB)
        # Reused by clone scenarios.
        # Hence comparing the stored size.
        if curr_size_bytes != new_size_bytes:
            lun_geometry = self.nclient.get_lun_geometry(path)
            if (lun_geometry and lun_geometry.get("max_resize")
                    and int(lun_geometry.get("max_resize")) >=
                    int(new_size_bytes)):
                self.nclient.do_direct_resize(path, new_size_bytes)
            else:
                self._do_sub_clone_resize(path, new_size_bytes)
            self.lun_table[name].size = new_size_bytes
        else:
            LOG.info(_("No need to extend volume %s"
                       " as it is already the requested new size."), name)

    def _get_vol_option(self, volume_name, option_name):
        """Get the value for the volume option."""
        value = None
        options = self.nclient.get_volume_options(volume_name)
        for opt in options:
            if opt.get_child_content('name') == option_name:
                value = opt.get_child_content('value')
                break
        return value

    def _do_sub_clone_resize(self, path, new_size_bytes):
        """Does sub lun clone after verification.

            Clones the block ranges and swaps
            the luns also deletes older lun
            after a successful clone.
        """
        seg = path.split("/")
        LOG.info(_("Resizing lun %s using sub clone to new size."), seg[-1])
        name = seg[-1]
        vol_name = seg[2]
        lun = self.lun_table[name]
        metadata = lun.metadata
        compression = self._get_vol_option(vol_name, 'compression')
        if compression == "on":
            msg = _('%s cannot be sub clone resized'
                    ' as it is hosted on compressed volume')
            raise exception.VolumeBackendAPIException(data=msg % name)
        else:
            block_count = self._get_lun_block_count(path)
            if block_count == 0:
                msg = _('%s cannot be sub clone resized'
                        ' as it contains no blocks.')
                raise exception.VolumeBackendAPIException(data=msg % name)
            new_lun = 'new-%s' % (name)
            self.nclient.create_lun(vol_name, new_lun, new_size_bytes,
                                    metadata)
            try:
                self._clone_lun(name, new_lun, block_count=block_count)
                self._post_sub_clone_resize(path)
            except Exception:
                with excutils.save_and_reraise_exception():
                    new_path = '/vol/%s/%s' % (vol_name, new_lun)
                    self.nclient.destroy_lun(new_path)

    def _post_sub_clone_resize(self, path):
        """Try post sub clone resize in a transactional manner."""
        st_tm_mv, st_nw_mv, st_del_old = None, None, None
        seg = path.split("/")
        LOG.info(_("Post clone resize lun %s"), seg[-1])
        new_lun = 'new-%s' % (seg[-1])
        tmp_lun = 'tmp-%s' % (seg[-1])
        tmp_path = "/vol/%s/%s" % (seg[2], tmp_lun)
        new_path = "/vol/%s/%s" % (seg[2], new_lun)
        try:
            st_tm_mv = self.nclient.move_lun(path, tmp_path)
            st_nw_mv = self.nclient.move_lun(new_path, path)
            st_del_old = self.nclient.destroy_lun(tmp_path)
        except Exception as e:
            if st_tm_mv is None:
                msg = _("Failure staging lun %s to tmp.")
                raise exception.VolumeBackendAPIException(data=msg % (seg[-1]))
            else:
                if st_nw_mv is None:
                    self.nclient.move_lun(tmp_path, path)
                    msg = _("Failure moving new cloned lun to %s.")
                    raise exception.VolumeBackendAPIException(
                        data=msg % (seg[-1]))
                elif st_del_old is None:
                    LOG.error(_("Failure deleting staged tmp lun %s."),
                              tmp_lun)
                else:
                    LOG.error(_("Unknown exception in"
                                " post clone resize lun %s."), seg[-1])
                    LOG.error(_("Exception details: %s") % (e.__str__()))

    def _get_lun_block_count(self, path):
        """Gets block counts for the lun."""
        LOG.debug(_("Getting lun block count."))
        block_count = 0
        lun_infos = self._get_lun_by_args(path=path)
        if not lun_infos:
            seg = path.split('/')
            msg = _('Failure getting lun info for %s.')
            raise exception.VolumeBackendAPIException(data=msg % seg[-1])
        lun_info = lun_infos[-1]
        bs = int(lun_info.get_child_content('block-size'))
        ls = int(lun_info.get_child_content('size'))
        block_count = ls / bs
        return block_count


class NetAppDirectCmodeISCSIDriver(NetAppDirectISCSIDriver):
    """NetApp C-mode iSCSI volume driver."""

    DEFAULT_VS = 'openstack'

    def __init__(self, *args, **kwargs):
        super(NetAppDirectCmodeISCSIDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(netapp_cluster_opts)

    def _do_custom_setup(self):
        """Does custom setup for ontap cluster."""
        self.vserver = self.configuration.netapp_vserver
        self.vserver = self.vserver if self.vserver else self.DEFAULT_VS
        self.nclient = cmode.Client(self.client, self.vserver)
        # We set vserver in client permanently.
        # To use tunneling enable_tunneling while invoking api
        self.client.set_vserver(self.vserver)
        # Default values to run first api
        self.client.set_api_version(1, 15)
        (major, minor) = self.nclient.get_ontapi_version()
        self.client.set_api_version(major, minor)
        self.ssc_vols = None
        self.stale_vols = set()

    def check_for_setup_error(self):
        """Check that the driver is working and can communicate."""
        ssc_utils.check_ssc_api_permissions(self.client)
        super(NetAppDirectCmodeISCSIDriver, self).check_for_setup_error()

    def _create_lun_on_eligible_vol(self, name, size, metadata,
                                    extra_specs=None):
        """Creates an actual lun on filer."""
        req_size = float(size) *\
            float(self.configuration.netapp_size_multiplier)
        qos_policy_group = None
        if extra_specs:
            qos_policy_group = extra_specs.pop('netapp:qos_policy_group', None)
        volumes = self._get_avl_volumes(req_size, extra_specs)
        if not volumes:
            msg = _('Failed to get vol with required'
                    ' size and extra specs for volume: %s')
            raise exception.VolumeBackendAPIException(data=msg % name)
        for volume in volumes:
            try:
                self.nclient.create_lun(volume.id['name'],
                                        name,
                                        size,
                                        metadata,
                                        qos_policy_group=qos_policy_group)
                metadata['Path'] = '/vol/%s/%s' % (volume.id['name'], name)
                metadata['Volume'] = volume.id['name']
                metadata['Qtree'] = None
                return
            except NaApiError as ex:
                msg = _("Error provisioning vol %(name)s on "
                        "%(volume)s. Details: %(ex)s")
                LOG.error(msg % {'name': name,
                                 'volume': volume.id['name'],
                                 'ex': ex})
            finally:
                self._update_stale_vols(volume=volume)

    def _get_avl_volumes(self, size, extra_specs=None):
        """Get the available volume by size, extra_specs."""
        result = []
        volumes = ssc_utils.get_volumes_for_specs(
            self.ssc_vols, extra_specs)
        if volumes:
            sorted_vols = sorted(volumes, reverse=True)
            for vol in sorted_vols:
                if int(vol.space['size_avl_bytes']) >= int(size):
                    result.append(vol)
        return result

    def _create_lun_handle(self, metadata):
        """Returns lun handle based on filer type."""
        return '%s:%s' % (self.vserver, metadata['Path'])

    def _find_mapped_lun_igroup(self, path, initiator, os=None):
        """Find the igroup for mapped lun with initiator."""
        initiator_igroups = self.nclient.get_igroup_by_initiator(
            initiator=initiator)
        lun_maps = self.nclient.get_lun_map(path)
        if initiator_igroups and lun_maps:
            for igroup in initiator_igroups:
                igroup_name = igroup['initiator-group-name']
                if igroup_name.startswith(self.IGROUP_PREFIX):
                    for lun_map in lun_maps:
                        if lun_map['initiator-group'] == igroup_name:
                            return (igroup_name, lun_map['lun-id'])
        return (None, None)

    def _clone_lun(self, name, new_name, space_reserved='true',
                   src_block=0, dest_block=0, block_count=0):
        """Clone LUN with the given handle to the new name."""
        metadata = self._get_lun_attr(name, 'metadata')
        volume = metadata['Volume']
        self.nclient.clone_lun(volume, name, new_name, space_reserved,
                               src_block=0, dest_block=0, block_count=0)
        LOG.debug(_("Cloned LUN with new name %s") % new_name)
        lun = self._get_lun_by_args(vserver=self.vserver, path='/vol/%s/%s'
                                    % (volume, new_name))
        if len(lun) == 0:
            msg = _("No cloned lun named %s found on the filer")
            raise exception.VolumeBackendAPIException(data=msg % (new_name))
        clone_meta = self._create_lun_meta(lun[0])
        self._add_lun_to_table(NetAppLun('%s:%s' % (clone_meta['Vserver'],
                                                    clone_meta['Path']),
                                         new_name,
                                         lun[0].get_child_content('size'),
                                         clone_meta))
        self._update_stale_vols(
            volume=ssc_utils.NetAppVolume(volume, self.vserver))

    def _get_lun_by_args(self, **args):
        """Retrieves lun with specified args."""
        lun_iter = NaElement('lun-get-iter')
        lun_iter.add_new_child('max-records', '100')
        query = NaElement('query')
        lun_iter.add_child_elem(query)
        query.add_node_with_children('lun-info', **args)
        luns = self.client.invoke_successfully(lun_iter)
        attr_list = luns.get_child_by_name('attributes-list')
        return attr_list.get_children()

    def _create_lun_meta(self, lun):
        """Creates lun metadata dictionary."""
        self._is_naelement(lun)
        meta_dict = {}
        self._is_naelement(lun)
        meta_dict['Vserver'] = lun.get_child_content('vserver')
        meta_dict['Volume'] = lun.get_child_content('volume')
        meta_dict['Qtree'] = lun.get_child_content('qtree')
        meta_dict['Path'] = lun.get_child_content('path')
        meta_dict['OsType'] = lun.get_child_content('multiprotocol-type')
        meta_dict['SpaceReserved'] = \
            lun.get_child_content('is-space-reservation-enabled')
        return meta_dict

    def _configure_tunneling(self, do_tunneling=False):
        """Configures tunneling for ontap cluster."""
        if do_tunneling:
            self.client.set_vserver(self.vserver)
        else:
            self.client.set_vserver(None)

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""

        LOG.debug(_("Updating volume stats"))
        data = {}
        netapp_backend = 'NetApp_iSCSI_Cluster_direct'
        backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = (
            backend_name or netapp_backend)
        data["vendor_name"] = 'NetApp'
        data["driver_version"] = '1.0'
        data["storage_protocol"] = 'iSCSI'

        data['total_capacity_gb'] = 0
        data['free_capacity_gb'] = 0
        data['reserved_percentage'] = 0
        data['QoS_support'] = False
        self._update_cluster_vol_stats(data)
        provide_ems(self, self.client, data, netapp_backend)
        self._stats = data

    def _update_cluster_vol_stats(self, data):
        """Updates vol stats with cluster config."""
        sync = True if self.ssc_vols is None else False
        ssc_utils.refresh_cluster_ssc(self, self.client, self.vserver,
                                      synchronous=sync)
        if self.ssc_vols:
            data['netapp_mirrored'] = 'true'\
                if self.ssc_vols['mirrored'] else 'false'
            data['netapp_unmirrored'] = 'true'\
                if len(self.ssc_vols['all']) > len(self.ssc_vols['mirrored'])\
                else 'false'
            data['netapp_dedup'] = 'true'\
                if self.ssc_vols['dedup'] else 'false'
            data['netapp_nodedup'] = 'true'\
                if len(self.ssc_vols['all']) > len(self.ssc_vols['dedup'])\
                else 'false'
            data['netapp_compression'] = 'true'\
                if self.ssc_vols['compression'] else 'false'
            data['netapp_nocompression'] = 'true'\
                if len(self.ssc_vols['all']) >\
                len(self.ssc_vols['compression'])\
                else 'false'
            data['netapp_thin_provisioned'] = 'true'\
                if self.ssc_vols['thin'] else 'false'
            data['netapp_thick_provisioned'] = 'true'\
                if len(self.ssc_vols['all']) >\
                len(self.ssc_vols['thin']) else 'false'
            if self.ssc_vols['all']:
                vol_max = max(self.ssc_vols['all'])
                data['total_capacity_gb'] =\
                    int(vol_max.space['size_total_bytes']) / units.GiB
                data['free_capacity_gb'] =\
                    int(vol_max.space['size_avl_bytes']) / units.GiB
            else:
                data['total_capacity_gb'] = 0
                data['free_capacity_gb'] = 0
        else:
            LOG.warn(_("Cluster ssc is not updated. No volume stats found."))

    @utils.synchronized('update_stale')
    def _update_stale_vols(self, volume=None, reset=False):
        """Populates stale vols with vol and returns set copy if reset."""
        if volume:
            self.stale_vols.add(volume)
        if reset:
            set_copy = copy.deepcopy(self.stale_vols)
            self.stale_vols.clear()
            return set_copy

    @utils.synchronized("refresh_ssc_vols")
    def refresh_ssc_vols(self, vols):
        """Refreshes ssc_vols with latest entries."""
        self.ssc_vols = vols

    def delete_volume(self, volume):
        """Driver entry point for destroying existing volumes."""
        lun = self.lun_table.get(volume['name'])
        netapp_vol = None
        if lun:
            netapp_vol = lun.get_metadata_property('Volume')
        super(NetAppDirectCmodeISCSIDriver, self).delete_volume(volume)
        if netapp_vol:
            self._update_stale_vols(
                volume=ssc_utils.NetAppVolume(netapp_vol, self.vserver))


class NetAppDirect7modeISCSIDriver(NetAppDirectISCSIDriver):
    """NetApp 7-mode iSCSI volume driver."""

    def __init__(self, *args, **kwargs):
        super(NetAppDirect7modeISCSIDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(netapp_7mode_opts)

    def _do_custom_setup(self):
        """Does custom setup depending on the type of filer."""
        self.vfiler = self.configuration.netapp_vfiler
        self.volume_list = self.configuration.netapp_volume_list
        if self.volume_list:
            self.volume_list = self.volume_list.split(',')
            self.volume_list = [el.strip() for el in self.volume_list]
        self.nclient = seven_mode.Client(self.client, self.volume_list)
        (major, minor) = self.nclient.get_ontapi_version()
        self.client.set_api_version(major, minor)
        if self.vfiler:
            self.client.set_vfiler(self.vfiler)
        self.vol_refresh_time = None
        self.vol_refresh_interval = 1800
        self.vol_refresh_running = False
        self.vol_refresh_voluntary = False
        # Setting it infinite at set up
        # This will not rule out backend from scheduling
        self.total_gb = 'infinite'
        self.free_gb = 'infinite'

    def check_for_setup_error(self):
        """Check that the driver is working and can communicate."""
        api_version = self.client.get_api_version()
        if api_version:
            major, minor = api_version
            if major == 1 and minor < 9:
                msg = _("Unsupported ONTAP version."
                        " ONTAP version 7.3.1 and above is supported.")
                raise exception.VolumeBackendAPIException(data=msg)
        else:
            msg = _("Api version could not be determined.")
            raise exception.VolumeBackendAPIException(data=msg)
        super(NetAppDirect7modeISCSIDriver, self).check_for_setup_error()

    def _create_lun_on_eligible_vol(self, name, size, metadata,
                                    extra_specs=None):
        """Creates an actual lun on filer."""
        req_size = float(size) *\
            float(self.configuration.netapp_size_multiplier)
        volume = self._get_avl_volume_by_size(req_size)
        if not volume:
            msg = _('Failed to get vol with required size for volume: %s')
            raise exception.VolumeBackendAPIException(data=msg % name)
        self.nclient.create_lun(volume['name'], name, size, metadata)
        metadata['Path'] = '/vol/%s/%s' % (volume['name'], name)
        metadata['Volume'] = volume['name']
        metadata['Qtree'] = None
        self.vol_refresh_voluntary = True

    def _get_filer_volumes(self, volume=None):
        """Returns list of filer volumes in api format."""
        vol_request = NaElement('volume-list-info')
        if volume:
            vol_request.add_new_child('volume', volume)
        res = self.client.invoke_successfully(vol_request, True)
        volumes = res.get_child_by_name('volumes')
        if volumes:
            return volumes.get_children()
        return []

    def _get_avl_volume_by_size(self, size):
        """Get the available volume by size."""
        vols = self._get_filer_volumes()
        for vol in vols:
            avl_size = vol.get_child_content('size-available')
            state = vol.get_child_content('state')
            if float(avl_size) >= float(size) and state == 'online':
                avl_vol = dict()
                avl_vol['name'] = vol.get_child_content('name')
                avl_vol['block-type'] = vol.get_child_content('block-type')
                avl_vol['type'] = vol.get_child_content('type')
                avl_vol['size-available'] = avl_size
                if self.volume_list:
                    if avl_vol['name'] in self.volume_list:
                        return avl_vol
                elif self._get_vol_option(avl_vol['name'], 'root') != 'true':
                        return avl_vol
        return None

    def _create_lun_handle(self, metadata):
        """Returns lun handle based on filer type."""
        if self.vfiler:
            owner = '%s:%s' % (self.configuration.netapp_server_hostname,
                               self.vfiler)
        else:
            owner = self.configuration.netapp_server_hostname
        return '%s:%s' % (owner, metadata['Path'])

    def _find_mapped_lun_igroup(self, path, initiator, os=None):
        """Find the igroup for mapped lun with initiator."""
        lun_map_list = NaElement.create_node_with_children(
            'lun-map-list-info',
            **{'path': path})
        result = self.client.invoke_successfully(lun_map_list, True)
        igroups = result.get_child_by_name('initiator-groups')
        if igroups:
            igroup = None
            lun_id = None
            found = False
            igroup_infs = igroups.get_children()
            for ig in igroup_infs:
                initiators = ig.get_child_by_name('initiators')
                init_infs = initiators.get_children()
                for info in init_infs:
                    if info.get_child_content('initiator-name') == initiator:
                        found = True
                        igroup = ig.get_child_content('initiator-group-name')
                        lun_id = ig.get_child_content('lun-id')
                        break
                if found:
                    break
        return (igroup, lun_id)

    def _clone_lun(self, name, new_name, space_reserved='true',
                   src_block=0, dest_block=0, block_count=0):
        """Clone LUN with the given handle to the new name."""
        metadata = self._get_lun_attr(name, 'metadata')
        path = metadata['Path']
        (parent, splitter, name) = path.rpartition('/')
        clone_path = '%s/%s' % (parent, new_name)

        self.nclient.clone_lun(path, clone_path, name, new_name,
                               space_reserved, src_block=0,
                               dest_block=0, block_count=0)

        self.vol_refresh_voluntary = True
        luns = self._get_lun_by_args(path=clone_path)
        if luns:
            cloned_lun = luns[0]
            self._set_space_reserve(clone_path, space_reserved)
            clone_meta = self._create_lun_meta(cloned_lun)
            handle = self._create_lun_handle(clone_meta)
            self._add_lun_to_table(
                NetAppLun(handle, new_name,
                          cloned_lun.get_child_content('size'),
                          clone_meta))
        else:
            raise NaApiError('ENOLUNENTRY', 'No Lun entry found on the filer')

    def _set_space_reserve(self, path, enable):
        """Sets the space reserve info."""
        space_res = NaElement.create_node_with_children(
            'lun-set-space-reservation-info',
            **{'path': path, 'enable': enable})
        self.client.invoke_successfully(space_res, True)

    def _get_lun_by_args(self, **args):
        """Retrieves luns with specified args."""
        lun_info = NaElement.create_node_with_children('lun-list-info', **args)
        result = self.client.invoke_successfully(lun_info, True)
        luns = result.get_child_by_name('luns')
        return luns.get_children()

    def _create_lun_meta(self, lun):
        """Creates lun metadata dictionary."""
        self._is_naelement(lun)
        meta_dict = {}
        self._is_naelement(lun)
        meta_dict['Path'] = lun.get_child_content('path')
        meta_dict['OsType'] = lun.get_child_content('multiprotocol-type')
        meta_dict['SpaceReserved'] = lun.get_child_content(
            'is-space-reservation-enabled')
        return meta_dict

    def _update_volume_stats(self):
        """Retrieve status info from volume group."""
        LOG.debug(_("Updating volume stats"))
        data = {}
        netapp_backend = 'NetApp_iSCSI_7mode_direct'
        backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = (
            backend_name or 'NetApp_iSCSI_7mode_direct')
        data["vendor_name"] = 'NetApp'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = 'iSCSI'
        data['reserved_percentage'] = 0
        data['QoS_support'] = False
        self._get_capacity_info(data)
        provide_ems(self, self.client, data, netapp_backend,
                    server_type="7mode")
        self._stats = data

    def _get_lun_block_count(self, path):
        """Gets block counts for the lun."""
        bs = super(
            NetAppDirect7modeISCSIDriver, self)._get_lun_block_count(path)
        api_version = self.client.get_api_version()
        if api_version:
            major = api_version[0]
            minor = api_version[1]
            if major == 1 and minor < 15:
                bs = bs - 1
        return bs

    def _get_capacity_info(self, data):
        """Calculates the capacity information for the filer."""
        if (self.vol_refresh_time is None or self.vol_refresh_voluntary or
                timeutils.is_newer_than(self.vol_refresh_time,
                                        self.vol_refresh_interval)):
            try:
                job_set = set_safe_attr(self, 'vol_refresh_running', True)
                if not job_set:
                    LOG.warn(
                        _("Volume refresh job already running. Returning..."))
                    return
                self.vol_refresh_voluntary = False
                self._refresh_capacity_info()
                self.vol_refresh_time = timeutils.utcnow()
            except Exception as e:
                LOG.warn(_("Error refreshing vol capacity. Message: %s"), e)
            finally:
                set_safe_attr(self, 'vol_refresh_running', False)
        data['total_capacity_gb'] = self.total_gb
        data['free_capacity_gb'] = self.free_gb

    def _refresh_capacity_info(self):
        """Gets the latest capacity information."""
        LOG.info(_("Refreshing capacity info for %s."), self.client)
        total_bytes = 0
        free_bytes = 0
        vols = self._get_filer_volumes()
        for vol in vols:
            volume = vol.get_child_content('name')
            if self.volume_list and not volume in self.volume_list:
                continue
            state = vol.get_child_content('state')
            inconsistent = vol.get_child_content('is-inconsistent')
            invalid = vol.get_child_content('is-invalid')
            if (state == 'online' and inconsistent == 'false'
                    and invalid == 'false'):
                total_size = vol.get_child_content('size-total')
                if total_size:
                    total_bytes = total_bytes + int(total_size)
                avl_size = vol.get_child_content('size-available')
                if avl_size:
                    free_bytes = free_bytes + int(avl_size)
        self.total_gb = total_bytes / units.GiB
        self.free_gb = free_bytes / units.GiB

    def delete_volume(self, volume):
        """Driver entry point for destroying existing volumes."""
        super(NetAppDirect7modeISCSIDriver, self).delete_volume(volume)
        self.vol_refresh_voluntary = True
