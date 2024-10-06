#!/usr/bin/env python3
# Patch /etc/one/oned.conf for Vitastor support
# -s = also enable save.vitastor/restore.vitastor overrides

import re
import os
import sys

class Fixer:
    save_restore = 0

    def require_sub_cb(self, m, cb):
        self.found = 1
        return cb(m)

    def require_sub(self, regexp, cb, text, error):
        self.found = 0
        new_text = re.sub(regexp, lambda m: self.require_sub_cb(m, cb), text)
        if not self.found and error:
            self.errors.append(error)
        return new_text

    def fix(self, oned_conf):
        self.errors = []
        self.kvm_found = 0
        oned_conf = self.require_sub(r'((?:^|\n)[ \t]*VM_MAD\s*=\s*\[)([^\]]+)\]', lambda m: m.group(1)+self.fix_vm_mad(m.group(2))+']', oned_conf, 'VM_MAD not found')
        if not self.kvm_found:
            self.errors.append("VM_MAD[NAME=kvm].ARGUMENTS not found")
        oned_conf = self.require_sub(r'((?:^|\n)[ \t]*TM_MAD\s*=\s*\[)([^\]]+)\]', lambda m: m.group(1)+self.fix_tm_mad(m.group(2))+']', oned_conf, 'TM_MAD not found')
        oned_conf = self.require_sub(r'((?:^|\n)[ \t]*DATASTORE_MAD\s*=\s*\[)([^\]]+)\]', lambda m: m.group(1)+self.fix_datastore_mad(m.group(2))+']', oned_conf, 'DATASTORE_MAD not found')
        if oned_conf[-1:] != '\n':
            oned_conf += '\n'
        if not re.compile(r'(^|\n)[ \t]*INHERIT_DATASTORE_ATTR\s*=\s*"VITASTOR_CONF"').search(oned_conf):
            oned_conf += '\nINHERIT_DATASTORE_ATTR="VITASTOR_CONF"\n'
        if not re.compile(r'(^|\n)[ \t]*INHERIT_DATASTORE_ATTR\s*=\s*"IMAGE_PREFIX"').search(oned_conf):
            oned_conf += '\nINHERIT_DATASTORE_ATTR="IMAGE_PREFIX"\n'
        if not re.compile(r'(^|\n)[ \t]*TM_MAD_CONF\s*=\s*\[[^\]]*NAME\s*=\s*"vitastor"').search(oned_conf):
            oned_conf += ('\nTM_MAD_CONF = [\n'+
                '    NAME = "vitastor", LN_TARGET = "NONE", CLONE_TARGET = "SELF", SHARED = "YES",\n'+
                '    DS_MIGRATE = "NO", DRIVER = "raw", ALLOW_ORPHANS="format",\n'+
                '    TM_MAD_SYSTEM = "ssh,shared", LN_TARGET_SSH = "SYSTEM", CLONE_TARGET_SSH = "SYSTEM",\n'+
                '    DISK_TYPE_SSH = "FILE", LN_TARGET_SHARED = "NONE",\n'+
                '    CLONE_TARGET_SHARED = "SELF", DISK_TYPE_SHARED = "FILE"\n'+
                ']\n')
        if not re.compile(r'(^|\n)[ \t]*DS_MAD_CONF\s*=\s*\[[^\]]*NAME\s*=\s*"vitastor"').search(oned_conf):
            oned_conf += ('\nDS_MAD_CONF = [\n'+
                '    NAME = "vitastor",\n'+
                '    REQUIRED_ATTRS = "DISK_TYPE,BRIDGE_LIST",\n'+
                '    PERSISTENT_ONLY = "NO",\n'+
                '    MARKETPLACE_ACTIONS = "export"\n'+
                ']\n')
        return oned_conf

    def fix_vm_mad(self, vm_mad_params):
        if re.compile(r'\bNAME\s*=\s*"kvm"').search(vm_mad_params):
            vm_mad_params = re.sub(r'\b(ARGUMENTS\s*=\s*")([^"]+)"', lambda m: m.group(1)+self.fix_vm_mad_args(m.group(2))+'"', vm_mad_params)
            self.kvm_found = 1
        return vm_mad_params

    def fix_vm_mad_args(self, args):
        args = self.fix_vm_mad_override(args, 'deploy')
        if self.save_restore:
            args = self.fix_vm_mad_override(args, 'save')
            args = self.fix_vm_mad_override(args, 'restore')
        return args

    def fix_vm_mad_override(self, args, override):
        m = re.compile(r'-l (\S+)').search(args)
        if m and re.compile(override+'='+override+'.vitastor').search(m.group(1)):
            return args
        elif m and re.compile(override+'=').search(m.group(1)):
            self.errors.append(override+"= is already overridden in -l option in VM_MAD[NAME=kvm].ARGUMENTS")
            return args
        elif m:
            return self.require_sub(r'-l (\S+)', lambda m: '-l '+m.group(1)+','+override+'='+override+'.vitastor', args, '-l option not found in VM_MAD[NAME=kvm].ARGUMENTS')
        else:
            return args+' -l '+override+'='+override+'.vitastor'

    def fix_tm_mad(self, params):
        return self.require_sub(r'\b(ARGUMENTS\s*=\s*")([^"]+)"', lambda m: m.group(1)+self.fix_tm_mad_args('d', m.group(2), "TM_MAD")+'"', params, "TM_MAD.ARGUMENTS not found")

    def fix_tm_mad_args(self, opt, args, v):
        return self.require_sub('(-'+opt+r') (\S+)', lambda m: self.fix_tm_mad_arg(m), args, "-"+opt+" option not found in "+v+".ARGUMENTS")

    def fix_tm_mad_arg(self, m):
        a = m.group(2).split(',')
        if 'vitastor' not in a:
            a += [ 'vitastor' ]
        return m.group(1)+' '+(','.join(a))

    def fix_datastore_mad(self, params):
        params = self.require_sub(r'\b(ARGUMENTS\s*=\s*")([^"]+)"', lambda m: m.group(1)+self.fix_tm_mad_args('d', m.group(2), "DATASTORE_MAD")+'"', params, "DATASTORE_MAD.ARGUMENTS not found")
        return self.require_sub(r'\b(ARGUMENTS\s*=\s*")([^"]+)"', lambda m: m.group(1)+self.fix_tm_mad_args('s', m.group(2), "DATASTORE_MAD")+'"', params, "")

fixer = Fixer()
oned_conf_file = ''
for arg in sys.argv[1:]:
    if arg == '-s':
        fixer.save_restore = 1
    else:
        oned_conf_file = arg
        break
if not oned_conf_file:
    sys.stderr.write("USAGE: ./patch-oned-conf.py [-s] /etc/one/oned.conf\n-s means also enable save.vitastor/restore.vitastor overrides\n")
    sys.exit(1)
with open(oned_conf_file, 'r') as fd:
    oned_conf = fd.read()
new_conf = fixer.fix(oned_conf)
if new_conf != oned_conf:
    os.rename(oned_conf_file, oned_conf_file+'.bak')
    with open(oned_conf_file, 'w') as fd:
        fd.write(new_conf)
if len(fixer.errors) > 0:
    sys.stderr.write("ERROR: Failed to patch "+oned_conf_file+", patch it manually. Errors:\n- "+('\n- '.join(fixer.errors))+'\n')
    sys.exit(1)
