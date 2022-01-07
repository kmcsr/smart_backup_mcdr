
import os
import time

import mcdreforged.api.all as MCDR
from .utils import *
from . import globals as GL
from .objects import *
from . import api

Prefix = '!!smb'

HelpMessage = '''
{0} help 显示帮助信息
{0} status 显示插件状态
{0} list [<limit>] 列出[所有/<limit>条]备份
{0} query <id> 查询备份详细信息
{0} make [<comment>] 创建新备份(差异/全盘)
{0} makefull [<comment>] 创建全盘备份
{0} rm <id> [force] 删除指定备份(及其子备份) #TODO
{0} restore <id> [force] 回档至指定备份
{0} confirm 确认操作
{0} abort 取消操作
{0} reload 重新加载配置文件
{0} save 保存配置文件
'''.strip().format(Prefix)

def register(server: MCDR.PluginServerInterface):
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
		then(GL.Config.literal('confirm').runs(command_confirm)).
		then(GL.Config.literal('abort').runs(command_abort)).
		then(GL.Config.literal('reload').runs(command_config_load)).
		then(GL.Config.literal('save').runs(command_config_save))
	)

def command_help(source: MCDR.CommandSource):
	send_block_message(source, HelpMessage)

@new_thread
def command_status(source: MCDR.CommandSource):
	lb = Backup.get_last(GL.Config.backup_path)
	lc = 'None' if lb is None else new_command(
		'{0} restore {1}'.format(Prefix, lb.id),
		'{0}: {1}({2})'.format(lb.id, lb.strftime, lb.comment))
	li = 0
	bs = 0
	if os.path.exists(GL.Config.backup_path):
		for a in os.listdir(GL.Config.backup_path):
			if a.startswith('0x'):
				li += 1
		bs = get_total_size(GL.Config.backup_path)
	send_block_message(source,
		'备份文件夹: ' + GL.Config.backup_path,
		'  大小: ' + format_size(bs),
		'  备份条数: ' + str(li),
		join_rtext('自动备份:', MCDR.RText('disabled' if api.backup_timer is None else 'enabled', color=MCDR.RColor.yellow)),
		join_rtext('最近一次备份:', lc)
	)

def command_list_backup(source: MCDR.CommandSource, limit: int):
	bks = Backup.list(GL.Config.backup_path, limit)
	lines = [MCDR.RTextList(b.z_index * '|',
		new_command('{0} restore {1}'.format(Prefix, b.id), b.id).h(join_rtext(
			'ID: ' + b.id,
			'Comment: ' + b.comment,
			'Date: ' + b.strftime,
			'Size: ' + format_size(get_total_size(os.path.join(GL.Config.backup_path, b.id))),
			sep='\n')),
		': ' + b.comment)
	for b in bks]
	send_block_message(source, '最后{}条备份:'.format(len(bks)), *lines)

@new_thread
def command_query_backup(source: MCDR.CommandSource, bid: str):
	if not bid.startswith('0x'):
		bid = '0x' + bid
	path = os.path.join(GL.Config.backup_path, bid)
	bk = Backup.load(path)
	if bk is None:
		send_message(source, MCDR.RText('Cannot find backup with id "{}"'.format(bid)))
		return
	send_block_message(source,
		'ID: ' + bk.id,
		'Comment: ' + bk.comment,
		'Date: ' + bk.strftime,
		'Size: ' + format_size(get_total_size(path)),
		join_rtext(
			new_command('{0} restore {1}'.format(Prefix, bk.id), '[回档]')
		)
	)

@new_thread
@new_job('make backup')
def command_make(source: MCDR.CommandSource, comment: str):
	server = source.get_server()
	broadcast_message('Making backup "{}"'.format(comment))

	next_job(api.make_backup, source, comment)

@new_thread
@new_job('make backup')
def command_makefull(source: MCDR.CommandSource, comment: str):
	server = source.get_server()
	broadcast_message('Making backup "{}"'.format(comment))

	next_job(api.make_backup, source, comment, mode=BackupMode.FULL)

@new_thread
@new_job('restore')
def command_restore(source: MCDR.CommandSource, bid: str, force: bool = False):
	if force:
		api.restore_backup(source, bid)
		return

	if not bid.startswith('0x'):
		bid = '0x' + bid
	path = os.path.join(GL.Config.backup_path, bid)
	bk = Backup.load(path)
	if bk is None:
		send_message(source, MCDR.RText('Cannot find backup with id "{}"'.format(bid)))
		return
	server = source.get_server()

	def pre_restore():
		abort: bool = False
		timeout: int = GL.Config.restore_timeout
		def ab():
			nonlocal abort
			abort = True
		date = bk.strftime
		register_confirm(None, lambda:0, ab)
		while timeout > 0:
			broadcast_message('{t} 秒后将重启回档至{date}({comment}), 输入'.format(t=timeout, date=date, comment=bk.comment),
				new_command('{} abort'.format(Prefix)), '撤销回档')
			time.sleep(1)
			if abort:
				broadcast_message('已取消回档')
				return
			timeout -= 1
		confirm_map.pop(None, None)
		next_job(lambda: api.restore_backup(source, bid))

	begin_job()
	register_confirm(source.player if source.is_player else '',
		new_thread(lambda: (pre_restore(), after_job())),
		lambda: (send_message(source, '已取消回档'), after_job()), timeout=15)
	send_message(source, MCDR.RText('确认回档至 "{}" 吗?'.format(bk.comment)).
		h('id: ' + bk.id,
			'comment: ' + bk.comment,
			'date: ' + bk.strftime,
			'size: ' + format_size(get_total_size(path))))
	send_message(source, '输入', new_command('{} confirm'.format(Prefix)), '确认, 输入',
		new_command('{} abort'.format(Prefix)), '取消')

def command_confirm(source: MCDR.CommandSource):
	confirm_map.pop(source.player if source.is_player else '', (lambda s: send_message(s, '当前没有正在执行的操作'), 0))[0](source)

def command_abort(source: MCDR.CommandSource):
	c = confirm_map.pop(source.player if source.is_player else '', (0, 0))[1]
	if not c:
		c = confirm_map.pop(None, (0, lambda s: send_message(s, '当前没有正在执行的操作')))[1]
	c(source)

@new_thread
def command_config_load(source: MCDR.CommandSource):
	GL.SMBConfig.load(source)

@new_thread
def command_config_save(source: MCDR.CommandSource):
	GL.Config.save()
	send_message(source, 'Save config file SUCCESS')

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
