
import re
import os
import json
from typing import List, Dict, Any

import mcdreforged.api.all as MCDR

from .objects import *

__all__ = [
	'MSG_ID', 'BIG_BLOCK_BEFOR', 'BIG_BLOCK_AFTER', 'SMBConfig', 'Config', 'SERVER_INS', 'init', 'destory'
]

MSG_ID = MCDR.RText('[SMB]', color=MCDR.RColor.green)
BIG_BLOCK_BEFOR = '------------ {0} v{1} ::::'
BIG_BLOCK_AFTER = ':::: {0} v{1} ============'

class SMBConfig(MCDR.Serializable):
	incremental_backup_limit: int = 12
	differential_backup_limit: int = 8
	full_backup_limit: int = 8
	full_backup_clean_indexs: List[int] = [1, 2, 2, 3, 3, 3, 0]
	backup_interval: int = 60 * 60 * 1 # 1 hours
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
	def cache(self):
		return self._cache

	@cache.setter
	def cache(self, cache):
		assert isinstance(cache, dict)
		self._cache = cache

	def get_next_clean_index(self):
		if 'clean_II' not in self.cache or self.cache['clean_II'] >= len(self.full_backup_clean_indexs):
			self.cache['clean_II'] = 0
		ind: int = self.full_backup_clean_indexs[self.cache['clean_II']]
		self.cache['clean_II'] = (self.cache['clean_II'] + 1) % len(self.full_backup_clean_indexs)
		return ind

	@classmethod
	def load(cls, source: MCDR.CommandSource = None):
		global Config, Manager
		cache: dict = {}
		oldConfig: SMBConfig = Config
		Config = SERVER_INS.load_config_simple(target_class=cls, source_to_reply=source)
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

	def save(self):
		SERVER_INS.save_config_simple(self)
		Manager.savecfg()
		if not os.path.exists(self.backup_path):
			os.makedirs(self.backup_path)
		with open(os.path.join(self.backup_path, 'cache.json'), 'w') as fd:
			json.dump(self._cache, fd, indent=4, separators=(', ', ': '), sort_keys=True)


Config: SMBConfig = None
Manager: BackupManager = None
SERVER_INS: MCDR.PluginServerInterface = None

on_load_callbacks = []
on_unload_callbacks = []

def on_load_call(call):
	on_load_callbacks.append(call)
	return call

def on_unload_call(call):
	on_unload_callbacks.append(call)
	return call

def init(server: MCDR.PluginServerInterface):
	global SERVER_INS
	SERVER_INS = server
	global BIG_BLOCK_BEFOR, BIG_BLOCK_AFTER
	metadata = server.get_self_metadata()
	BIG_BLOCK_BEFOR = BIG_BLOCK_BEFOR.format(metadata.name, metadata.version)
	BIG_BLOCK_AFTER = BIG_BLOCK_AFTER.format(metadata.name, metadata.version)
	SMBConfig.load()
	for c in on_load_callbacks:
		c(server)

def destory():
	global SERVER_INS, Manager
	if Config is not None:
		Config.save()
	Manager = None
	for c in on_unload_callbacks:
		c(SERVER_INS)
	SERVER_INS = None
