
import re
import os
import json
from typing import List, Dict, Any

import mcdreforged.api.all as MCDR

from .objects import *

__all__ = [
	'MSG_ID', 'BIG_BLOCK_BEFOR', 'BIG_BLOCK_AFTER', 'SMBConfig', 'Config', 'init', 'destory'
]

MSG_ID = MCDR.RText('[SMB]', color=MCDR.RColor.green)
BIG_BLOCK_BEFOR = '------------ {0} v{1} ::::'
BIG_BLOCK_AFTER = ':::: {0} v{1} ============'

class SMBConfig(MCDR.Serializable):
	incremental_backup_limit: int = 2
	differential_backup_limit: int = 4
	full_backup_limit: int = 8
	full_backup_protect_times: List[int] = [ # unit minute
		60 * 24 * 15, # 15 days
		60 * 24 * 7, # 1 week
		60 * 24, # 1 day
		60 * 24 * 3,
		60 * 24,
		0, # 0 sec (non-protection)
		60 * 24 * 3,
		60 * 24,
	]
	backup_interval: int = 60 * 60 * 1 # 1 hour
	restore_timeout: int = 30
	backup_path: str = './smt_backups'
	overwrite_path: str = './smt_backup_overwrite'
	backup_needs: List[str] = ['world']
	backup_ignores: List[str] = ['session.lock']
	befor_backup: List[str] = ['save-off', 'save-all flush']
	start_backup_trigger_info: str = r'Saved the (?:game|world)'
	after_backup: List[str] = ['save-on']
	# 0:guest 1:user 2:helper 3:admin 4:owner
	minimum_permission_level: Dict[str, int] = {
		'help':     0,
		'status':   1,
		'list':     1,
		'query':    1,
		'make':     2,
		'makefull': 3,
		'rm':       3,
		'restore':  3,
		'confirm':  1,
		'abort':    1,
		'reload':   3,
		'save':     3,
	}

	def __init__(self):
		super().__init__()
		self._server = None

	def test_backup_trigger(self, info: str):
		if not hasattr(self, '__start_backup_trigger') or self.__start_backup_trigger_info != self.start_backup_trigger_info:
			self.__start_backup_trigger_info = self.start_backup_trigger_info
			self.__start_backup_trigger = re.compile(self.start_backup_trigger_info)
		return self.__start_backup_trigger.fullmatch(info) is not None

	def literal(self, literal: str):
		lvl = self.minimum_permission_level.get(literal, 4)
		return MCDR.Literal(literal).requires(lambda src: src.has_permission(lvl),
			lambda: MCDR.RTextList(MSG_ID, MCDR.RText(' permission denied', color=MCDR.RColor.red)))

	@property
	def server(self):
		return self._server

	@property
	def cache(self):
		return self._cache

	@cache.setter
	def cache(self, cache):
		assert isinstance(cache, dict)
		self._cache = cache

	def get_next_protect_time(self):
		pti: int = self.cache['protect_time_ind'] + 1 if 'protect_time_ind' in self.cache else 0
		if pti > len(self.full_backup_protect_times):
			if pti < self.full_backup_limit:
				return 0
			pti = 0
		self.cache['protect_time_ind'] = pti
		return self.full_backup_protect_times[pti] * 60

	def _fix(self):
		pass # TODO

	@classmethod
	def load(cls, source: MCDR.CommandSource, server: MCDR.PluginServerInterface = None):
		global Config, Manager
		cache: dict = {}
		oldConfig: SMBConfig = Config
		if server is None:
			assert oldConfig != None
			server = oldConfig._server
		Config = server.load_config_simple(target_class=cls, source_to_reply=source)
		Config._server = server
		Config._fix()

		cf: str = os.path.join(Config.backup_path, 'cache.json')
		if os.path.exists(cf):
			with open(cf, 'r') as fd:
				try:
					cache = json.load(fd)
				except Exception as e:
					cache = {} if oldConfig is None else oldConfig.cache
		Config.cache = cache
		if oldConfig is None or oldConfig.backup_path != Config.backup_path:
			Manager = BackupManager(Config.backup_path)

	def save(self, source: MCDR.CommandSource):
		self._server.save_config_simple(self)
		Manager.savecfg()
		if not os.path.exists(self.backup_path):
			os.makedirs(self.backup_path)
		with open(os.path.join(self.backup_path, 'cache.json'), 'w') as fd:
			json.dump(self._cache, fd, indent=4, separators=(', ', ': '), sort_keys=True)
		source.reply('Config file save SUCCESS')


Config: SMBConfig = None
Manager: BackupManager = None

on_load_callbacks = []
on_unload_callbacks = []

def on_load_call(call):
	on_load_callbacks.append(call)
	return call

def on_unload_call(call):
	on_unload_callbacks.append(call)
	return call

def init(server: MCDR.PluginServerInterface):
	global BIG_BLOCK_BEFOR, BIG_BLOCK_AFTER
	metadata = server.get_self_metadata()
	BIG_BLOCK_BEFOR = BIG_BLOCK_BEFOR.format(metadata.name, metadata.version)
	BIG_BLOCK_AFTER = BIG_BLOCK_AFTER.format(metadata.name, metadata.version)
	SMBConfig.load(server.get_plugin_command_source(), server)
	for c in on_load_callbacks:
		c(server)

def destory(server: MCDR.PluginServerInterface):
	global Manager
	if Config is not None:
		Config.save(server.get_plugin_command_source())
		Config = None
	Manager = None
