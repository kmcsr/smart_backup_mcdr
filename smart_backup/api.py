
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
	make_backup(source, time.strftime('SMB timed backup: %Y-%m-%d %H:%M:%S', time.localtime()))

@GL.on_load_call
def on_load(server: MCDR.PluginServerInterface):
	_flush_backup_timer()

@GL.on_unload_call
def on_unload(server: MCDR.PluginServerInterface):
	global backup_timer
	if backup_timer is not None:
		backup_timer.cancel()
		backup_timer = None

def on_server_start(server: MCDR.PluginServerInterface):
	_clear_job()

@new_job('clean up backup')
def clean_backup():
	if GL.Config.full_backup_limit < 1:
		broadcast_message(MCDR.RText('[ERROR] full_backup_limit is less than one, cannot do clean up', color=MCDR.RColor.red))
		return
	broadcast_message('Cleaning backup...')
	start_time = time.time()
	before_size = get_total_size(GL.Config.backup_path)
	while len(GL.Manager.index.fulln) > GL.Config.full_backup_limit:
		bid = GL.Manager.index.fulln[GL.Config.get_next_clean_index()]
		bk = GL.Manager.load(bid)
		broadcast_message('Removing backup {id}:{comment}({date})...'.format(id=bk.id, comment=bk.comment, date=bk.strftime))
		bk.remove()
	used_time = time.time() - start_time
	free_size = before_size - get_total_size(GL.Config.backup_path)
	broadcast_message('Backup cleaned up, use {0:.2f} sec, free up disk {1}'.format(used_time, format_size(free_size)))

@new_job('make backup')
def make_backup(source: MCDR.CommandSource, comment: str, mode: BackupMode = None, clean: bool = True):
	cancel_backup_timer()
	server = source.get_server()
	broadcast_message('Making backup "{}"'.format(comment))
	start_time = time.time()
	def c():
		nonlocal mode, start_time
		if mode is None:
			prev: Backup = GL.Manager.get_last()
			mode = BackupMode.FULL
			if prev is not None:
				if 'incremental_count' in GL.Config.cache and \
					GL.Config.cache['incremental_count'] < GL.Config.incremental_backup_limit:
					GL.Config.cache['incremental_count'] += 1
					mode = BackupMode.INCREMENTAL
				elif 'differential_count' in GL.Config.cache and \
					GL.Config.cache['differential_count'] < GL.Config.differential_backup_limit:
					GL.Config.cache['differential_count'] += 1
					mode = BackupMode.DIFFERENTIAL
		if mode == BackupMode.FULL:
			GL.Config.cache['incremental_count'] = 0
			GL.Config.cache['differential_count'] = 0
		elif mode == BackupMode.DIFFERENTIAL:
			GL.Config.cache['incremental_count'] = 0

		backup = GL.Manager.create(mode, comment,
			source.get_server().get_mcdr_config()['working_directory'], GL.Config.backup_needs, GL.Config.backup_ignores, saved=False)
		send_message(source, 'Saving backup "{}"'.format(comment), log=True)
		backup.save()
		send_message(source, 'Saved backup "{}"'.format(comment), log=True)
		for _ in map(server.execute, GL.Config.after_backup): pass
		used_time = time.time() - start_time
		broadcast_message('Backup finished, use {:.2f} sec'.format(used_time))
		_flush_backup_timer()
		if clean and mode == BackupMode.FULL and GL.Config.full_backup_limit > 0 and len(GL.Manager.index.fulln) > GL.Config.full_backup_limit:
			broadcast_message('Backup out of range, automagically cleaning backups...')
			next_job_call(clean_backup)

	if len(GL.Config.start_backup_trigger_info) > 0:
		ping_job()
		global game_saved_callback
		game_saved_callback = new_thread(after_job_wrapper(c))
		for _ in map(server.execute, GL.Config.befor_backup): pass
	else:
		for _ in map(server.execute, GL.Config.befor_backup): pass
		c()

@new_job('restore')
def restore_backup(source: MCDR.CommandSource, bid: str):
	if not bid.startswith('0x'):
		bid = '0x' + bid
	try:
		bk = GL.Manager.load(bid)
	except BackupNotFoundError:
		send_message(source, MCDR.RText('Cannot find backup with id "{}"'.format(bid), color=MCDR.RColor.red))
		return False
	server = source.get_server()

	broadcast_message('Stopping the server')
	server.stop()
	server.wait_for_start()
	make_backup(source, f'Server before restore({bid}) backup', mode=BackupMode.FULL, clean=False)
	log_info('Restoring...')
	bk.restore(server.get_mcdr_config()['working_directory'], GL.Config.backup_needs, GL.Config.backup_ignores)
	log_info('Starting the server')
	server.start()

	return True

@new_job('remove')
def remove_backup(source: MCDR.CommandSource, bid: str):
	if not bid.startswith('0x'):
		bid = '0x' + bid
	try:
		bk = GL.Manager.load(bid)
	except BackupNotFoundError:
		send_message(source, MCDR.RText('Cannot find backup with id "{}"'.format(bid), color=MCDR.RColor.red))
		return False
	server = source.get_server()

	bk.remove()
	broadcast_message('<{0}> removed backup {1}({2})'.format(source, bk.strftime, bk.comment))
	return True
