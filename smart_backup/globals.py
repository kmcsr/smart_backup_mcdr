
import re
from typing import List, Dict, Any

import mcdreforged.api.all as MCDR

__all__ = [
	'MSG_ID', 'BIG_BLOCK_BEFOR', 'BIG_BLOCK_AFTER', 'SMBConfig', 'Config', 'SERVER_INS', 'init', 'destory'
]

MSG_ID = MCDR.RText('[SMB]', color=MCDR.RColor.green)
BIG_BLOCK_BEFOR = '------------ {0} v{1} ::::'
BIG_BLOCK_AFTER = ':::: {0} v{1} ============'

class SMBConfig(MCDR.Serializable):
	differential_backup_limit: int = 10
	full_backup_limit: int = 10
	backup_interval: int = 60 * 60 * 1 # 1 hours
	last_backup_time: int = 0
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
		'make':     2,
		'makefull': 3,
		'back':     3,
		'confirm':  1,
		'abort':    1,
		'reload':   3,
		'save':     3,
	}
	_cache: Dict[str, Any] = {}

	def test_backup_trigger(self, info: str):
		if not hasattr(self, '__start_backup_trigger') or self.__start_backup_trigger_info != self.start_backup_trigger_info:
			self.__start_backup_trigger_info = self.start_backup_trigger_info
			self.__start_backup_trigger = re.compile(self.start_backup_trigger_info)
		return self.__start_backup_trigger.fullmatch(info) is not None

	@property
	def cache(self):
		return self._cache

	def literal(self, literal: str):
		lvl = self.minimum_permission_level.get(literal, 0)
		return MCDR.Literal(literal).requires(lambda src: src.has_permission(lvl),
			lambda: MCDR.RText(MSG_ID.to_plain_text() + ' 权限不足', color=MCDR.RColor.red))

	def save(self):
		SERVER_INS.save_config_simple(self)


Config: SMBConfig = SMBConfig()
SERVER_INS: MCDR.PluginServerInterface = None

def init(server: MCDR.PluginServerInterface):
	global SERVER_INS
	SERVER_INS = server
	global BIG_BLOCK_BEFOR, BIG_BLOCK_AFTER
	metadata = server.get_self_metadata()
	BIG_BLOCK_BEFOR = BIG_BLOCK_BEFOR.format(metadata.name, metadata.version)
	BIG_BLOCK_AFTER = BIG_BLOCK_AFTER.format(metadata.name, metadata.version)
	global Config
	Config = server.load_config_simple(target_class=SMBConfig)

def destory():
	global SERVER_INS
	Config.save()
	SERVER_INS = None
