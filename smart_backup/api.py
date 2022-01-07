
import os
import time

import mcdreforged.api.all as MCDR
from .utils import *
from . import globals as GL
from .objects import *

__all__ = [
	'make_backup', 'restore_backup'
]

game_saved_callback = None

def on_info(server: MCDR.ServerInterface, info: MCDR.Info):
	if not info.is_user:
		global game_saved_callback
		if game_saved_callback is not None and GL.Config.test_backup_trigger(info.content):
			c, game_saved_callback = game_saved_callback, None
			c()

backup_timer = None

def cancel_backup_timer():
	global backup_timer
	if backup_timer is not None:
		backup_timer.cancel()
		backup_timer = None

def _flush_backup_timer():
	global backup_timer
	cancel_backup_timer()
	if GL.Config.backup_interval > 0:
		broadcast_message('Next backup time: ' +
			time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + GL.Config.backup_interval)))
		backup_timer = new_timer(GL.Config.backup_interval, _timed_make_backup)

def _timed_make_backup():
	global backup_timer
	backup_timer = None
	source = GL.SERVER_INS.get_plugin_command_source()
	cmt = 'SMB timed backup: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
	broadcast_message('Making backup "{}"'.format(cmt))
	try:
		make_backup(source, cmt)
	finally:
		if backup_timer is None:
			_flush_backup_timer()

@GL.on_load_call
def on_load(server: MCDR.PluginServerInterface):
	_flush_backup_timer()

@GL.on_unload_call
def on_unload(server: MCDR.PluginServerInterface):
	global backup_timer
	if backup_timer is not None:
		backup_timer.cancel()
		backup_timer = None

@new_job('make backup')
def make_backup(source: MCDR.CommandSource, comment: str, mode: BackupMode = None):
	cancel_backup_timer()
	server = source.get_server()
	start_time = time.time()
	tuple(map(server.execute, GL.Config.befor_backup))
	def c():
		nonlocal mode
		prev: Backup = None
		if mode is None:
			mode = BackupMode.FULL
			if 'differential_count' not in GL.Config.cache or GL.Config.cache['differential_count'] >= GL.Config.differential_backup_limit:
				GL.Config.cache['differential_count'] = 0
			else:
				prev = Backup.get_last(GL.Config.backup_path)
				if prev is None:
					GL.Config.cache['differential_count'] = 0
				else:
					GL.Config.cache['differential_count'] += 1
					mode = BackupMode.DIFFERENTIAL
		backup = Backup.create(mode, comment,
			source.get_server().get_mcdr_config()['working_directory'], GL.Config.backup_needs, GL.Config.backup_ignores, prev=prev)
		send_message(source, 'Saving backup "{}"'.format(comment), log=True)
		backup.save(GL.Config.backup_path)
		send_message(source, 'Saved backup "{}"'.format(comment), log=True)
		tuple(map(server.execute, GL.Config.after_backup))
		used_time = time.time() - start_time
		broadcast_message('Backup finished, use {:.2f} sec'.format(used_time))
		_flush_backup_timer()

	if len(GL.Config.start_backup_trigger_info) > 0:
		begin_job()
		global game_saved_callback
		game_saved_callback = new_thread(lambda: (c(), after_job()))
		tuple(map(server.execute, GL.Config.befor_backup))
	else:
		tuple(map(server.execute, GL.Config.befor_backup))
		c()

@new_job('restore')
def restore_backup(source: MCDR.CommandSource, bid: str):
	if not bid.startswith('0x'):
		bid = '0x' + bid
	path = os.path.join(GL.Config.backup_path, bid)
	bk = Backup.load(path)
	if bk is None:
		send_message(source, MCDR.RText('Cannot find backup with id "{}"'.format(bid), color=MCDR.RColor.red))
		return False
	server = source.get_server()

	broadcast_message('Stopping the server')
	server.stop()
	server.wait_for_start()
	log_info('Restoring...')
	bk.restore(server.get_mcdr_config()['working_directory'], GL.Config.backup_needs, GL.Config.backup_ignores)
	log_info('Starting the server')
	server.start()

	return True


