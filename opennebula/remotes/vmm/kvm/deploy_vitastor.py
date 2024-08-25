#!/usr/bin/env python3

# Vitastor OpenNebula driver
# Copyright (c) Vitaliy Filippov, 2024+
# License: Apache-2.0 http://www.apache.org/licenses/LICENSE-2.0

import base64
from sys import argv, stderr
from xml.etree import ElementTree as ET

dep_file = argv[1]
with open(dep_file, 'rb') as fd:
    dep_txt = base64.b64decode(fd.read())
dep = ET.fromstring(dep_txt)

vm_file = argv[2]
with open(vm_file, 'rb') as fd:
    vm_txt = base64.b64decode(fd.read())
vm = ET.fromstring(vm_txt)

ET.register_namespace('qemu', 'http://libvirt.org/schemas/domain/qemu/1.0')
ET.register_namespace('one', 'http://opennebula.org/xmlns/libvirt/1.0')

vm_id = vm.find('./ID').text
context_disk_id = vm.find('./TEMPLATE/CONTEXT/DISK_ID').text
changed = 0
txt = lambda x: '' if x is None else x.text

for disk in dep.findall('./devices/disk[@type="file"]'):
    try:
        disk_id = disk.find('./source').attrib['file'].split('.')[-1]
        vm_disk = vm.find('./TEMPLATE/DISK[DISK_ID="{}"]'.format(disk_id))
        if vm_disk is None:
            continue
        tm_mad = txt(vm_disk.find('./TM_MAD'))
        if tm_mad != 'vitastor':
            continue
        src_image = txt(vm_disk.find('./SOURCE'))
        clone = txt(vm_disk.find('./CLONE'))
        vitastor_conf = txt(vm_disk.find('./VITASTOR_CONF'))
        if clone == "YES":
            src_image += "-"+vm_id+"-"+disk_id
        # modify
        changed = 1
        disk.attrib['type'] = 'network'
        disk.remove(disk.find('./source'))
        src = ET.SubElement(disk, 'source')
        src.attrib['protocol'] = 'vitastor'
        src.attrib['name'] = src_image
        if vitastor_conf:
            # path to config should be added to /etc/apparmor.d/local/abstractions/libvirt-qemu
            config = ET.SubElement(src, 'config')
            config.text = vitastor_conf
    except Exception as e:
        print("Error: {}".format(e), file=stderr)

if changed:
    ET.ElementTree(dep).write(dep_file)
