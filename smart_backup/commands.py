
import os
import time

import mcdreforged.api.all as MCDR
from .utils import *
from . import globals as GL
from .objects import *
from . import api

Prefix = '!!smb'

HelpMessage: str = None

def register(server: MCDR.PluginServerInterface):
	global HelpMessage
	HelpMessage = tr('help_msg', Prefix)

	server.register_command(
		MCDR.Literal(Prefix).
		runs(command_help).
		then(GL.Config.literal('help').runs(command_help)).
		then(GL.Config.literal('status').runs(command_status)).
		then(GL.Config.literal('list').
			runs(lambda src: command_list_backup(src, 10)).
			then(MCDR.Integer('limit').at_min(0).runs(lambda src, ctx: command_list_backup(src, ctx['limit'])))).
		then(GL.Config.literal('query').
			then(MCDR.Text('id').runs(lambda src, ctx: command_query_backup(src, ctx['id'])))).
		then(GL.Config.literal('make').
			runs(lambda src: command_make(src, 'None')).
			then(MCDR.GreedyText('comment').runs(lambda src, ctx: command_make(src, ctx['comment'])))).
		then(GL.Config.literal('makefull').
			runs(lambda src: command_makefull(src, 'None')).
			then(MCDR.GreedyText('comment').runs(lambda src, ctx: command_makefull(src, ctx['comment'])))).
		then(GL.Config.literal('restore').
			then(MCDR.Text('id').runs(lambda src, ctx: command_restore(src, ctx['id'])).
				then(MCDR.Boolean('force').runs(lambda src, ctx: command_restore(src, ctx['id'], ctx['force']))))).
		then(GL.Config.literal('remove').
			then(MCDR.Text('id').runs(lambda src, ctx: command_remove(src, ctx['id'])).
				then(MCDR.Boolean('force').runs(lambda src, ctx: command_remove(src, ctx['id'], ctx['force']))))).
		then(GL.Config.literal('confirm').runs(command_confirm)).
		then(GL.Config.literal('abort').runs(command_abort)).
		then(GL.Config.literal('reload').runs(command_config_load)).
		then(GL.Config.literal('save').runs(command_config_save))
	)

def command_help(source: MCDR.CommandSource):
	send_block_message(source, HelpMessage)

@new_thread
def command_status(source: MCDR.CommandSource):
	lb = GL.Manager.get_last()
	lc = 'None' if lb is None else new_command(
		f'{Prefix} restore {lb.id}',
		f'{lb.id}: {lb.strftime}({lb.comment})')
	bs = 0
	if os.path.exists(GL.Config.backup_path):
		bs = get_total_size(GL.Config.backup_path)
	send_block_message(source,
		'Backup path: ' + GL.Config.backup_path,
		'  Size: ' + format_size(bs),
		'  Count: ' + str(len(GL.Manager.listID())),
		join_rtext('Timed backup:', MCDR.RText('disabled' if api.backup_timer is None else 'enabled', color=MCDR.RColor.yellow)),
		join_rtext('Last backup:', lc)
	)

@new_thread
def command_list_backup(source: MCDR.CommandSource, limit: int):
	bks = GL.Manager.list(limit)
	send_message(source, GL.BIG_BLOCK_BEFOR)
	send_message(source, 'Last backups (up to {} lines):'.format(limit))
	for b in bks:
		send_message(source, MCDR.RTextList(b.z_index * '|',
			new_command(f'{Prefix} restore {b.id}', b.id).h(tr('query.status',
				id=b.id,
				comment=b.comment,
				date=b.strftime,
				size=format_size(get_total_size(os.path.join(GL.Config.backup_path, b.id)))
			)),
			': ' + b.comment))
	send_message(source, GL.BIG_BLOCK_AFTER)

@new_thread
def command_query_backup(source: MCDR.CommandSource, bid: str):
	if not bid.startswith('0x'):
		bid = '0x' + bid
	try:
		bk = GL.Manager.load(bid)
	except BackupNotFoundError:
		send_message(source, MCDR.RText(tr('error.not_found', bid), color=MCDR.RColor.red))
		return
	send_block_message(source, tr('query.status',
			id=bk.id,
			comment=bk.comment,
			date=bk.strftime,
			size=format_size(get_total_size(os.path.join(GL.Config.backup_path, bk.id)))
		),
		join_rtext(
			new_command(f'{Prefix} restore {bk.id}', f'[{tr("restore.word")}]'),
			new_command(f'{Prefix} remove {bk.id}', f'[{tr("remove.word")}]'),
			sep=' | '
		)
	)

@new_thread
def command_make(source: MCDR.CommandSource, comment: str):
	api.make_backup(source, comment)

@new_thread
def command_makefull(source: MCDR.CommandSource, comment: str):
	api.make_backup(source, comment, mode=BackupMode.FULL)

@new_thread
@new_job('restore')
def command_restore(source: MCDR.CommandSource, bid: str, force: bool = False):
	if force:
		swap_job_call(api.restore_backup, source, bid)
		return

	if not bid.startswith('0x'):
		bid = '0x' + bid
	try:
		bk = GL.Manager.load(bid)
	except BackupNotFoundError:
		send_message(source, MCDR.RText(tr('error.not_found', bid), color=MCDR.RColor.red))
		return
	server = source.get_server()

	@new_thread
	@after_job_wrapper
	def pre_restore():
		abort: bool = False
		timeout: int = GL.Config.restore_timeout
		def ab():
			nonlocal abort
			abort = True
		register_confirm(None, lambda:0, ab)
		while timeout > 0:
			broadcast_message(tr('restore.restart_note', t=timeout, date=bk.strftime, comment=bk.comment) + ',',
				tr('word.run'), new_command(f'{Prefix} abort'), tr('word.to_cancel'))
			time.sleep(1)
			if abort:
				broadcast_message(tr('restore.canceled'))
				return
			timeout -= 1
		confirm_map.pop(None, None)
		swap_job_call(api.restore_backup, source, bid)

	ping_job()
	register_confirm(source.player if source.is_player else '',
		pre_restore,
		after_job_wrapper(lambda: send_message(source, tr('restore.canceled'))), timeout=15)
	send_message(source, tr(date=bk.strftime, comment=bk.comment).
		h(tr('query.status',
			id=bk.id,
			comment=bk.comment,
			date=bk.strftime,
			size=format_size(get_total_size(os.path.join(GL.Config.backup_path, bk.id)))
		)))
	send_message(source, tr('word.run'), new_command(f'{Prefix} confirm'), tr('to_confirm') + ',',
		tr('word.run'), new_command(f'{Prefix} abort'), tr('word.to_cancel'))

@new_thread
@new_job('remove')
def command_remove(source: MCDR.CommandSource, bid: str, force: bool = False):
	if force:
		swap_job_call(api.remove_backup, source, bid)
		return

	if not bid.startswith('0x'):
		bid = '0x' + bid
	try:
		bk = GL.Manager.load(bid)
	except BackupNotFoundError:
		send_message(source, MCDR.RText(tr('error.not_found', bid), color=MCDR.RColor.red))
		return
	server = source.get_server()

	ping_job()
	register_confirm(source.player if source.is_player else '',
		after_job_wrapper(lambda: swap_job_call(api.remove_backup, source, bid)),
		after_job_wrapper(lambda: send_message(source, 'Canceled removing')), timeout=15)
	send_message(source, tr('remove.ask', date=bk.strftime, comment=bk.comment).
		h(tr('query.status',
			id=bk.id,
			comment=bk.comment,
			date=bk.strftime,
			size=format_size(get_total_size(os.path.join(GL.Config.backup_path, bk.id)))
		)))
	send_message(source, tr('word.run'), new_command(f'{Prefix} confirm'), tr('to_confirm') + ',',
		tr('word.run'), new_command(f'{Prefix} abort'), tr('word.to_cancel'))

def command_confirm(source: MCDR.CommandSource):
	confirm_map.pop(source.player if source.is_player else '', (lambda s: send_message(s, tr('word.no_action')), 0))[0](source)

def command_abort(source: MCDR.CommandSource):
	c = confirm_map.pop(source.player if source.is_player else '', (0, 0))[1]
	if not c:
		c = confirm_map.pop(None, (0, lambda s: send_message(s, tr('word.no_action'))))[1]
	c(source)

@new_thread
def command_config_load(source: MCDR.CommandSource):
	GL.SMBConfig.load(source)

@new_thread
def command_config_save(source: MCDR.CommandSource):
	GL.Config.save(source)

confirm_map = {}

def __warp_call(call, c2=None):
	def c(*b):
		if c2 is not None:
			c2()
		return call(*b[:call.__code__.co_argcount])
	return c

def register_confirm(player: str, confirm_call, abort_call=lambda: 0, timeout: int=None):
	if timeout is not None:
		tmc = new_timer(timeout, lambda: confirm_map.pop(player, (0, lambda: 0))[1]()).cancel
	else:
		tmc = lambda: 0
	confirm_map[player] = (__warp_call(confirm_call, tmc), __warp_call(abort_call, tmc))
