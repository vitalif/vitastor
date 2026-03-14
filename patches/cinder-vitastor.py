# Vitastor Driver for OpenStack Cinder
#
# --------------------------------------------
# Install as cinder/volume/drivers/vitastor.py
# --------------------------------------------
#
# Copyright 2020 Vitaliy Filippov
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Cinder Vitastor Driver"""

import binascii
import base64
import errno
import json
import math
import os
import tempfile

from castellan import key_manager
from oslo_config import cfg
from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_concurrency import processutils
from oslo_utils import encodeutils
from oslo_utils import excutils
from oslo_utils import fileutils
from oslo_utils import units
import six
from six.moves.urllib import request

from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import objects
from cinder.objects import fields
from cinder import utils
from cinder.volume import configuration
from cinder.volume import driver
from cinder.volume import volume_utils

VITASTOR_VERSION = '3.0.5'

LOG = logging.getLogger(__name__)

VITASTOR_OPTS = [
    cfg.StrOpt(
        'vitastor_config_path',
        default='/etc/vitastor/vitastor.conf',
        help='Vitastor configuration file path'
    ),
    cfg.StrOpt(
        'vitastor_etcd_address',
        default='',
        help='Vitastor etcd address(es)'),
    cfg.StrOpt(
        'vitastor_etcd_prefix',
        default='/vitastor',
        help='Vitastor etcd prefix'
    ),
    cfg.StrOpt(
        'vitastor_pool_id',
        default='',
        help='Vitastor pool ID to use for volumes'
    ),
    # FIXME exclusive_cinder_pool ?
]

CONF = cfg.CONF
CONF.register_opts(VITASTOR_OPTS, group = configuration.SHARED_CONF_GROUP)

class VitastorDriverException(exception.VolumeDriverException):
    message = _("Vitastor Cinder driver failure: %(reason)s")

@interface.volumedriver
class VitastorDriver(driver.CloneableImageVD,
    driver.ManageableVD, driver.ManageableSnapshotsVD,
    driver.BaseVD):
    """Implements Vitastor volume commands."""

    cfg = {}
    _etcd_urls = []

    def __init__(self, active_backend_id = None, *args, **kwargs):
        super(VitastorDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(VITASTOR_OPTS)

    @classmethod
    def get_driver_options(cls):
        additional_opts = cls._get_oslo_driver_opts(
            'reserved_percentage',
            'max_over_subscription_ratio',
            'volume_dd_blocksize'
        )
        return VITASTOR_OPTS + additional_opts

    def do_setup(self, context):
        """Performs initialization steps that could raise exceptions."""
        super(VitastorDriver, self).do_setup(context)
        # Make sure configuration is in UTF-8
        for attr in [ 'config_path', 'etcd_address', 'etcd_prefix', 'pool_id' ]:
            val = self.configuration.safe_get('vitastor_'+attr)
            if val is not None:
                self.cfg[attr] = utils.convert_str(val)
        self.cfg = self._load_config(self.cfg)

    def _load_config(self, cfg):
        # Try to load configuration file
        try:
            f = open(cfg['config_path'] or '/etc/vitastor/vitastor.conf')
            conf = json.loads(f.read())
            f.close()
            for k in conf:
                cfg[k] = cfg.get(k, conf[k])
        except:
            pass
        if isinstance(cfg['etcd_address'], str):
            cfg['etcd_address'] = cfg['etcd_address'].split(',')
        # Sanitize etcd URLs
        for i, etcd_url in enumerate(cfg['etcd_address']):
            ssl = False
            if etcd_url.lower().startswith('http://'):
                etcd_url = etcd_url[7:]
            elif etcd_url.lower().startswith('https://'):
                etcd_url = etcd_url[8:]
                ssl = True
            if etcd_url.find('/') < 0:
                etcd_url += '/v3'
            if ssl:
                etcd_url = 'https://'+etcd_url
            else:
                etcd_url = 'http://'+etcd_url
            cfg['etcd_address'][i] = etcd_url
        return cfg

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met."""

    def _encode_etcd_key(self, key):
        if not isinstance(key, bytes):
            key = str(key).encode('utf-8')
        return base64.b64encode(self.cfg['etcd_prefix'].encode('utf-8')+b'/'+key).decode('utf-8')

    def _encode_etcd_value(self, value):
        if not isinstance(value, bytes):
            value = str(value).encode('utf-8')
        return base64.b64encode(value).decode('utf-8')

    def _encode_etcd_requests(self, obj):
        for v in obj:
            for rt in v:
                if 'key' in v[rt]:
                    v[rt]['key'] = self._encode_etcd_key(v[rt]['key'])
                if 'range_end' in v[rt]:
                    v[rt]['range_end'] = self._encode_etcd_key(v[rt]['range_end'])
                if 'value' in v[rt]:
                    v[rt]['value'] = self._encode_etcd_value(v[rt]['value'])

    def _etcd_txn(self, params):
        if 'compare' in params:
            for v in params['compare']:
                if 'key' in v:
                    v['key'] = self._encode_etcd_key(v['key'])
        if 'failure' in params:
            self._encode_etcd_requests(params['failure'])
        if 'success' in params:
            self._encode_etcd_requests(params['success'])
        body = json.dumps(params).encode('utf-8')
        headers = {
            'Content-Type': 'application/json'
        }
        err = None
        for etcd_url in self.cfg['etcd_address']:
            try:
                resp = request.urlopen(request.Request(etcd_url+'/kv/txn', body, headers), timeout = 5)
                data = json.loads(resp.read())
                if 'responses' not in data:
                    data['responses'] = []
                for i, resp in enumerate(data['responses']):
                    if 'response_range' in resp:
                        if 'kvs' not in resp['response_range']:
                            resp['response_range']['kvs'] = []
                        for kv in resp['response_range']['kvs']:
                            kv['key'] = base64.b64decode(kv['key'].encode('utf-8')).decode('utf-8')
                            if kv['key'].startswith(self.cfg['etcd_prefix']+'/'):
                                kv['key'] = kv['key'][len(self.cfg['etcd_prefix'])+1 : ]
                            kv['value'] = json.loads(base64.b64decode(kv['value'].encode('utf-8')))
                    if len(resp.keys()) != 1:
                        LOG.exception('unknown responses['+str(i)+'] format: '+json.dumps(resp))
                    else:
                        resp = data['responses'][i] = resp[list(resp.keys())[0]]
                return data
            except Exception as e:
                LOG.exception('error calling etcd transaction: '+body.decode('utf-8')+'\nerror: '+str(e))
                err = e
        raise err

    def _etcd_foreach(self, prefix, add_fn):
        total = 0
        batch = 1000
        begin = prefix+'/'
        while True:
            resp = self._etcd_txn({ 'success': [
                { 'request_range': {
                    'key': begin,
                    'range_end': prefix+'0',
                    'limit': batch+1,
                } },
            ] })
            i = 0
            while i < batch and i < len(resp['responses'][0]['kvs']):
                kv = resp['responses'][0]['kvs'][i]
                add_fn(kv)
                i += 1
            if len(resp['responses'][0]['kvs']) <= batch:
                break
            begin = resp['responses'][0]['kvs'][batch]['key']
        return total

    def _update_volume_stats(self):
        location_info = json.dumps({
            'config': self.configuration.vitastor_config_path,
            'etcd_address': self.configuration.vitastor_etcd_address,
            'etcd_prefix': self.configuration.vitastor_etcd_prefix,
            'pool_id': self.configuration.vitastor_pool_id,
        })

        stats = {
            'vendor_name': 'Vitastor',
            'driver_version': VITASTOR_VERSION,
            'storage_protocol': 'vitastor',
            'total_capacity_gb': 'unknown',
            'free_capacity_gb': 'unknown',
            # FIXME check if safe_get is required
            'reserved_percentage': self.configuration.safe_get('reserved_percentage'),
            'multiattach': True,
            'thin_provisioning_support': True,
            'max_over_subscription_ratio': self.configuration.safe_get('max_over_subscription_ratio'),
            'location_info': location_info,
            'backend_state': 'down',
            'volume_backend_name': self.configuration.safe_get('volume_backend_name') or 'vitastor',
            'replication_enabled': False,
        }

        try:
            pool_stats = self._etcd_txn({ 'success': [
                { 'request_range': { 'key': 'pool/stats/'+str(self.cfg['pool_id']) } }
            ] })
            total_provisioned = 0
            def add_total(kv):
                nonlocal total_provisioned
                if kv['key'].find('@') >= 0:
                    total_provisioned += kv['value']['size']
            self._etcd_foreach('config/inode/'+str(self.cfg['pool_id']), lambda kv: add_total(kv))
            stats['provisioned_capacity_gb'] = round(total_provisioned/1024.0/1024.0/1024.0, 2)
            pool_stats = pool_stats['responses'][0]['kvs']
            if len(pool_stats):
                pool_stats = pool_stats[0]['value']
                stats['free_capacity_gb'] = round(1024.0*(pool_stats['total_raw_tb']-pool_stats['used_raw_tb'])/pool_stats['raw_to_usable'], 2)
                stats['total_capacity_gb'] = round(1024.0*pool_stats['total_raw_tb'], 2)
            stats['backend_state'] = 'up'
        except Exception as e:
            # just log and return unknown capacities
            LOG.exception('error getting vitastor pool stats: '+str(e))

        self._stats = stats

    def get_volume_stats(self, refresh=False):
        """Get volume stats.
        If 'refresh' is True, run update the stats first.
        """
        if not self._stats or refresh:
            self._update_volume_stats()

        return self._stats

    def _next_id(self, resp):
        if len(resp['kvs']) == 0:
            return (1, 0)
        else:
            return (1 + resp['kvs'][0]['value'], resp['kvs'][0]['mod_revision'])

    def _cli(self, descr, *args):
        args = [ 'vitastor-cli', *args, *(self._vitastor_args()) ]
        try:
            self._execute(*args)
        except processutils.ProcessExecutionError as exc:
            LOG.error("Failed to "+descr+": "+exc)
            raise exception.VolumeBackendAPIException(data = exc.stderr)

    def create_volume(self, volume):
        """Creates a logical volume."""

        size = int(volume.size) * units.Gi
        # FIXME: Check if convert_str is really required
        vol_name = utils.convert_str(volume.name)
        if vol_name.find('@') >= 0 or vol_name.find('/') >= 0:
            raise exception.VolumeBackendAPIException(data = '@ and / are forbidden in volume and snapshot names')

        LOG.debug("creating volume '%s'", vol_name)

        self._cli('create volume', 'create', vol_name, '--size', size)

        if volume.encryption_key_id:
            self._create_encrypted_volume(volume, volume.obj_context)

        volume_update = {}
        return volume_update

    def _create_encrypted_volume(self, volume, context):
        """Create a new LUKS encrypted image directly in Vitastor."""
        vol_name = utils.convert_str(volume.name)
        f, opts = self._encrypt_opts(volume, context)
        # FIXME: Check if it works at all :-)
        self._execute(
            'qemu-img', 'convert', '-f', 'luks', *opts,
            'vitastor:image='+vol_name.replace(':', '\\:')+self._qemu_args(),
            '%sM' % (volume.size * 1024)
        )
        f.close()

    def _encrypt_opts(self, volume, context):
        encryption = volume_utils.check_encryption_provider(self.db, volume, context)
        # Fetch the key associated with the volume and decode the passphrase
        keymgr = key_manager.API(CONF)
        key = keymgr.get(context, encryption['encryption_key_id'])
        passphrase = binascii.hexlify(key.get_encoded()).decode('utf-8')
        # Decode the dm-crypt style cipher spec into something qemu-img can use
        cipher_spec = image_utils.decode_cipher(encryption['cipher'], encryption['key_size'])
        tmp_dir = volume_utils.image_conversion_dir()
        f = tempfile.NamedTemporaryFile(prefix = 'luks_', dir = tmp_dir)
        f.write(passphrase)
        f.flush()
        return (f, [
            '--object', 'secret,id=luks_sec,format=raw,file=%(passfile)s' % {'passfile': f.name},
            '-o', 'key-secret=luks_sec,cipher-alg=%(cipher_alg)s,cipher-mode=%(cipher_mode)s,ivgen-alg=%(ivgen_alg)s' % cipher_spec,
        ])

    def create_snapshot(self, snapshot):
        """Creates a volume snapshot."""

        vol_name = utils.convert_str(snapshot.volume_name)
        snap_name = utils.convert_str(snapshot.name)
        if snap_name.find('@') >= 0 or snap_name.find('/') >= 0:
            raise exception.VolumeBackendAPIException(data = '@ and / are forbidden in volume and snapshot names')
        self._cli('create snapshot', 'snap-create', vol_name+'@'+snap_name)

    def snapshot_revert_use_temp_snapshot(self):
        """Disable the use of a temporary snapshot on revert."""
        return False

    def revert_to_snapshot(self, context, volume, snapshot):
        """Revert a volume to a given snapshot."""

        vol_name = utils.convert_str(snapshot.volume_name)
        snap_name = utils.convert_str(snapshot.name)

        # Delete the image and recreate it from the snapshot
        self._cli('delete image', 'rm', vol_name)
        self._cli('recreate image', 'create', '--parent', vol_name+'@'+snap_name, vol_name)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""

        vol_name = utils.convert_str(snapshot.volume_name)
        snap_name = utils.convert_str(snapshot.name)

        self._cli('remove snapshot', 'rm', vol_name+'@'+snap_name)

    def _child_count(self, parents):
        children = 0
        def add_child(kv):
            nonlocal children
            children += self._check_parent(kv, parents)
        self._etcd_foreach('config/inode', lambda kv: add_child(kv))
        return children

    def _check_parent(self, kv, parents):
        if 'parent_id' not in kv['value']:
            return 0
        parent_id = kv['value']['parent_id']
        _, _, pool_id, inode_id = kv['key'].split('/')
        parent_pool_id = pool_id
        if 'parent_pool_id' in kv['value'] and kv['value']['parent_pool_id']:
            parent_pool_id = kv['value']['parent_pool_id']
        inode = (int(pool_id) << 48) | (int(inode_id) & 0xffffffffffff)
        parent = (int(parent_pool_id) << 48) | (int(parent_id) & 0xffffffffffff)
        if parent in parents and inode not in parents:
            return 1
        return 0

    def create_cloned_volume(self, volume, src_vref):
        """Create a cloned volume from another volume."""

        size = int(volume.size) * units.Gi
        src_name = utils.convert_str(src_vref.name)
        dest_name = utils.convert_str(volume.name)
        if dest_name.find('@') >= 0 or dest_name.find('/') >= 0:
            raise exception.VolumeBackendAPIException(data = '@ and / are forbidden in volume and snapshot names')

        # FIXME Do full copy if requested (cfg.disable_clone)

        if src_vref.admin_metadata.get('readonly') == 'True':
            # source volume is a volume-image cache entry or other readonly volume
            # clone without intermediate snapshot
            self._cli('create clone', 'create', '--parent', src_name, '--size', size, dest_name)
            return {}

        clone_snap = "%s@%s.clone_snap" % (src_name, dest_name)
        make_img = True
        if (volume.display_name and
            volume.display_name.startswith('image-') and
            src_vref.project_id != volume.project_id):
            # idiotic openstack creates image-volume cache entries
            # as clones of normal VM volumes... :-X prevent it :-D
            clone_snap = dest_name
            make_img = False

        LOG.debug("creating snapshot '%s'", clone_snap)
        self._cli('create base snapshot', 'snap-create', '--allow-existing', '1', clone_snap)

        if make_img:
            # Then create a clone from it
            self._cli('create clone', 'create', '--parent', clone_snap, '--size', size, dest_name)

        return {}

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a cloned volume from an existing snapshot."""

        vol_name = utils.convert_str(volume.name)
        snap_name = utils.convert_str(snapshot.name)

        src_snap = 'volume-'+snapshot.volume_id+'@'+snap_name
        snap = self._get_image(src_snap)
        if not snap:
            raise exception.SnapshotNotFound(snapshot_id = snap_name)
        snap_inode_id = int(resp['responses'][0]['kvs'][0]['value']['id'])
        snap_pool_id = int(resp['responses'][0]['kvs'][0]['value']['pool_id'])

        size = snap['cfg']['size']
        if int(volume.size):
            size = int(volume.size) * units.Gi

        self._cli('create clone', 'create', vol_name, '--size', size, '--parent', src_snap)
        return {}

    def _vitastor_args(self):
        args = []
        for k in [ 'config_path', 'etcd_address', 'etcd_prefix' ]:
            v = self.configuration.safe_get('vitastor_'+k)
            if v:
                args.extend(['--'+k, v])
        return args

    def _qemu_args(self):
        args = ''
        for k in [ 'config_path', 'etcd_address', 'etcd_prefix' ]:
            v = self.configuration.safe_get('vitastor_'+k)
            kk = k
            if kk == 'etcd_address':
                # FIXME use etcd_address in qemu driver
                kk = 'etcd_host'
            if v:
                args += ':'+kk.replace('_', '-')+'='+v.replace(':', '\\:')
        return args

    def delete_volume(self, volume):
        """Deletes a logical volume."""

        vol_name = utils.convert_str(volume.name)
        self._cli('delete volume', 'rm', '--matching', vol_name, vol_name+'@*', '--progress', '0')

    def retype(self, context, volume, new_type, diff, host):
        """Change extra type specifications for a volume."""

        # FIXME Maybe (in the future) support multiple pools as different types
        return True, {}

    def ensure_export(self, context, volume):
        """Synchronously recreates an export for a logical volume."""
        pass

    def create_export(self, context, volume, connector):
        """Exports the volume."""
        pass

    def remove_export(self, context, volume):
        """Removes an export for a logical volume."""
        pass

    def initialize_connection(self, volume, connector):
        data = {
            'driver_volume_type': 'vitastor',
            'data': {
                'config_path': self.configuration.vitastor_config_path,
                'etcd_address': self.configuration.vitastor_etcd_address,
                'etcd_prefix': self.configuration.vitastor_etcd_prefix,
                'name': volume.name,
                'logical_block_size': '512',
                'physical_block_size': '4096',
            }
        }
        LOG.debug('connection data: %s', data)
        return data

    def terminate_connection(self, volume, connector, **kwargs):
        pass

    def clone_image(self, context, volume, image_location, image_meta, image_service):
        if image_location:
            # Note: image_location[0] is glance image direct_url.
            # image_location[1] contains the list of all locations (including
            # direct_url) or None if show_multiple_locations is False in
            # glance configuration.
            if image_location[1]:
                url_locations = [location['url'] for location in image_location[1]]
            else:
                url_locations = [image_location[0]]
            # iterate all locations to look for a cloneable one.
            for url_location in url_locations:
                if url_location and url_location.startswith('cinder://'):
                    # The idea is to use cinder://<volume-id> Glance volumes as base images
                    base_vol = self.db.volume_get(context, url_location[len('cinder://') : ])
                    if not base_vol or base_vol.volume_type_id != volume.volume_type_id:
                        continue
                    size = int(volume.size) * units.Gi
                    dest_name = utils.convert_str(volume.name)
                    # Find or create the base snapshot
                    self._cli('create base snapshot', 'create', '--allow-existing', '1', base_vol.name+'@.clone_snap')
                    # Then create a clone from it
                    self._cli('create clone', 'create', dest_name, '--size', size, '--parent', base_vol.name+'@.clone_snap')
                    return ({}, True)
        return ({}, False)

    def copy_image_to_encrypted_volume(self, context, volume, image_service, image_id, disable_sparse=False):
        self.copy_image_to_volume(context, volume, image_service, image_id, encrypted = True, disable_sparse=False)

    def copy_image_to_volume(self, context, volume, image_service, image_id, encrypted = False, disable_sparse=False):
        tmp_dir = volume_utils.image_conversion_dir()
        with tempfile.NamedTemporaryFile(dir = tmp_dir) as tmp:
            image_utils.fetch_to_raw(
                context, image_service, image_id, tmp.name,
                self.configuration.volume_dd_blocksize, size = volume.size
            )
            out_format = [ '-O', 'raw' ]
            if encrypted:
                key_file, opts = self._encrypt_opts(volume, context)
                out_format = [ '-O', 'luks', *opts ]
            dest_name = utils.convert_str(volume.name)
            self._try_execute(
                'qemu-img', 'convert', '-f', 'raw', tmp.name, *out_format,
                'vitastor:image='+dest_name.replace(':', '\\:')+self._qemu_args()
            )
            if encrypted:
                key_file.close()

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        tmp_dir = volume_utils.image_conversion_dir()
        tmp_file = os.path.join(tmp_dir, volume.name + '-' + image_meta['id'])
        with fileutils.remove_path_on_error(tmp_file):
            vol_name = utils.convert_str(volume.name)
            self._try_execute(
                'qemu-img', 'convert', '-f', 'raw',
                'vitastor:image='+vol_name.replace(':', '\\:')+self._qemu_args(),
                '-O', 'raw', tmp_file
            )
            # FIXME: Copy directly if the destination image is also in Vitastor
            volume_utils.upload_volume(context, image_service, image_meta, tmp_file, volume)
        os.unlink(tmp_file)

    def _get_image(self, vol_name):
        # find the image
        resp = self._etcd_txn({ 'success': [
            { 'request_range': { 'key': 'index/image/'+vol_name } },
        ] })
        if len(resp['responses'][0]['kvs']) == 0:
            return None
        vol_idx = resp['responses'][0]['kvs'][0]['value']
        vol_idx_mod = resp['responses'][0]['kvs'][0]['mod_revision']
        # get image inode config
        resp = self._etcd_txn({ 'success': [
            { 'request_range': { 'key': 'config/inode/'+str(vol_idx['pool_id'])+'/'+str(vol_idx['id']) } },
        ] })
        if len(resp['responses'][0]['kvs']) == 0:
            return None
        vol_cfg = resp['responses'][0]['kvs'][0]['value']
        vol_cfg_mod = resp['responses'][0]['kvs'][0]['mod_revision']
        return {
            'cfg': vol_cfg,
            'cfg_mod': vol_cfg_mod,
            'idx': vol_idx,
            'idx_mod': vol_idx_mod,
        }

    def extend_volume(self, volume, new_size):
        """Extend an existing volume."""
        vol_name = utils.convert_str(volume.name)
        size = int(new_size) * units.Gi
        self._cli('extend volume', 'modify', vol_name, '--resize', new_size)
        LOG.debug(
            "Extend volume from %(old_size)s GB to %(new_size)s GB.",
            {'old_size': volume.size, 'new_size': new_size}
        )

    def _add_manageable_volume(self, kv, manageable_volumes, cinder_ids):
        cfg = kv['value']
        if kv['key'].find('@') >= 0:
            # snapshot
            return
        image_id = volume_utils.extract_id_from_volume_name(cfg['name'])
        image_info = {
            'reference': {'source-name': image_name},
            'size': int(math.ceil(float(cfg['size']) / units.Gi)),
            'cinder_id': None,
            'extra_info': None,
        }
        if image_id in cinder_ids:
            image_info['cinder_id'] = image_id
            image_info['safe_to_manage'] = False
            image_info['reason_not_safe'] = 'already managed'
        else:
            image_info['safe_to_manage'] = True
            image_info['reason_not_safe'] = None
        manageable_volumes.append(image_info)

    def get_manageable_volumes(self, cinder_volumes, marker, limit, offset, sort_keys, sort_dirs):
        manageable_volumes = []
        cinder_ids = [resource['id'] for resource in cinder_volumes]

        # List all volumes
        # FIXME: It's possible to use pagination in our case, but.. do we want it?
        self._etcd_foreach('config/inode/'+str(self.cfg['pool_id']),
            lambda kv: self._add_manageable_volume(kv, manageable_volumes, cinder_ids))

        return volume_utils.paginate_entries_list(
            manageable_volumes, marker, limit, offset, sort_keys, sort_dirs)

    def _get_existing_name(existing_ref):
        if not isinstance(existing_ref, dict):
            existing_ref = {"source-name": existing_ref}
        if 'source-name' not in existing_ref:
            reason = _('Reference must contain source-name element.')
            raise exception.ManageExistingInvalidReference(existing_ref=existing_ref, reason=reason)
        src_name = utils.convert_str(existing_ref['source-name'])
        if not src_name:
            reason = _('Reference must contain source-name element.')
            raise exception.ManageExistingInvalidReference(existing_ref=existing_ref, reason=reason)
        return src_name

    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of an existing image for manage_existing.

        :param volume: volume ref info to be set
        :param existing_ref: {'source-name': <image name>}
        """
        src_name = self._get_existing_name(existing_ref)
        vol = self._get_image(src_name)
        if not vol:
            raise exception.VolumeBackendAPIException(data = 'Volume '+src_name+' does not exist')
        return int(math.ceil(float(vol['cfg']['size']) / units.Gi))

    def manage_existing(self, volume, existing_ref):
        """Manages an existing image.

        Renames the image name to match the expected name for the volume.

        :param volume: volume ref info to be set
        :param existing_ref: {'source-name': <image name>}
        """
        from_name = self._get_existing_name(existing_ref)
        to_name = utils.convert_str(volume.name)
        self._cli('rename', 'modify', from_name, '--rename', to_name)

    def unmanage(self, volume):
        pass

    def _add_manageable_snapshot(self, kv, manageable_snapshots, cinder_ids):
        cfg = kv['value']
        dog = kv['key'].find('@')
        if dog < 0:
            # snapshot
            return
        image_name = kv['key'][0 : dog]
        snap_name = kv['key'][dog+1 : ]
        snapshot_id = volume_utils.extract_id_from_snapshot_name(snap_name)
        snapshot_info = {
            'reference': {'source-name': snap_name},
            'size': int(math.ceil(float(cfg['size']) / units.Gi)),
            'cinder_id': None,
            'extra_info': None,
            'safe_to_manage': False,
            'reason_not_safe': None,
            'source_reference': {'source-name': image_name}
        }
        if snapshot_id in cinder_ids:
            # Exclude snapshots already managed.
            snapshot_info['reason_not_safe'] = ('already managed')
            snapshot_info['cinder_id'] = snapshot_id
        elif snap_name.endswith('.clone_snap'):
            # Exclude clone snapshot.
            snapshot_info['reason_not_safe'] = ('used for clone snap')
        else:
            snapshot_info['safe_to_manage'] = True
        manageable_snapshots.append(snapshot_info)

    def get_manageable_snapshots(self, cinder_snapshots, marker, limit, offset, sort_keys, sort_dirs):
        """List manageable snapshots in Vitastor."""
        manageable_snapshots = []
        cinder_snapshot_ids = [resource['id'] for resource in cinder_snapshots]
        # List all volumes
        # FIXME: It's possible to use pagination in our case, but.. do we want it?
        self._etcd_foreach('config/inode/'+str(self.cfg['pool_id']),
            lambda kv: self._add_manageable_volume(kv, manageable_snapshots, cinder_snapshot_ids))
        return volume_utils.paginate_entries_list(
            manageable_snapshots, marker, limit, offset, sort_keys, sort_dirs)

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        """Return size of an existing image for manage_existing.

        :param snapshot: snapshot ref info to be set
        :param existing_ref: {'source-name': <name of snapshot>}
        """
        vol_name = utils.convert_str(snapshot.volume_name)
        snap_name = self._get_existing_name(existing_ref)
        vol = self._get_image(vol_name+'@'+snap_name)
        if not vol:
            raise exception.ManageExistingInvalidReference(
                existing_ref=snapshot_name, reason='Specified snapshot does not exist.'
            )
        return int(math.ceil(float(vol['cfg']['size']) / units.Gi))

    def manage_existing_snapshot(self, snapshot, existing_ref):
        """Manages an existing snapshot.

        Renames the snapshot name to match the expected name for the snapshot.
        Error checking done by manage_existing_get_size is not repeated.

        :param snapshot: snapshot ref info to be set
        :param existing_ref: {'source-name': <name of snapshot>}
        """
        vol_name = utils.convert_str(snapshot.volume_name)
        snap_name = self._get_existing_name(existing_ref)
        from_name = vol_name+'@'+snap_name
        to_name = vol_name+'@'+utils.convert_str(snapshot.name)
        self._cli('rename', 'modify', from_name, '--rename', to_name)

    def unmanage_snapshot(self, snapshot):
        """Removes the specified snapshot from Cinder management."""
        pass

    def _dumps(self, obj):
        return json.dumps(obj, separators=(',', ':'), sort_keys=True)
