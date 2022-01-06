
import mcdreforged.api.all as MCDR
from .utils import *
from . import globals as GL
from .objects import *

Prefix = '!!smb'

HelpMessage = '''
{0} help 显示帮助信息
{0} list [<limit>] 列出[所有/<limit>条]备份
{0} query <id> 查询备份详细信息
{0} make [<comment>] 创建新备份(差异/全盘)
{0} makefull [<comment>] 创建全盘备份
{0} rm <id> [force] 删除指定备份(及其子备份)
{0} restore [<id>] 回档至[上次/指定id]备份
{0} confirm 确认操作
{0} abort 取消操作
{0} reload 重新加载配置文件
{0} save 保存配置文件
'''.strip().format(Prefix)

game_saved_callback = None

def on_info(server: MCDR.ServerInterface, info: MCDR.Info):
	if not info.is_user:
		global game_saved_callback
		if game_saved_callback is not None and GL.Config.test_backup_trigger(info.content):
			c, game_saved_callback = game_saved_callback, None
			c()

def register(server: MCDR.PluginServerInterface):
	server.register_command(
		MCDR.Literal(Prefix).
		runs(command_help).
		then(GL.Config.literal('help').runs(command_help)).
		then(GL.Config.literal('list').
			runs(lambda src: command_list_backup(src, 10)).
			then(MCDR.Integer('limit').at_min(0).
				runs(lambda src, ctx: command_list_backup(src, ctx['limit'])))).
		then(GL.Config.literal('query').
			then(MCDR.Text('id').
				runs(lambda src, ctx: command_query_backup(src, ctx['id'])))).
		then(GL.Config.literal('make').
			runs(lambda src: command_make(src, 'None')).
			then(MCDR.GreedyText('comment').runs(lambda src, ctx: command_make(src, ctx['comment'])))).
		then(GL.Config.literal('makefull').
			runs(lambda src: command_makefull(src, 'None')).
			then(MCDR.GreedyText('comment').runs(lambda src, ctx: command_makefull(src, ctx['comment'])))).
		then(GL.Config.literal('confirm').runs(command_confirm)).
		then(GL.Config.literal('abort').runs(command_abort)).
		then(GL.Config.literal('reload').runs(command_config_load)).
		then(GL.Config.literal('save').runs(command_config_save))
	)

def command_help(source: MCDR.CommandSource):
	send_block_message(source, HelpMessage)

def command_list_backup(source: MCDR.CommandSource, limit: int):
	send_message(source, 'TODO: list "{}" backup'.format(limit))

@new_thread
def command_query_backup(source: MCDR.CommandSource, bid: str):
	send_message(source, 'TODO: query backup "{}"'.format(bid))

@new_thread
@new_job('make backup')
def command_make(source: MCDR.CommandSource, comment: str):
	server = source.get_server()
	send_message(source, 'Making backup "{}"'.format(comment), log=True)
	tuple(map(server.execute, GL.Config.befor_backup))

	def call():
		mode = BackupMode.FULL
		if 'differential_count' not in GL.Config.cache or GL.Config.cache['differential_count'] >= GL.Config.differential_backup_limit:
			GL.Config.cache['differential_count'] = 0
		else:
			mode = BackupMode.DIFFERENTIAL
			GL.Config.cache['differential_count'] += 1
		backup = Backup.create(mode, comment,
			source.get_server().get_mcdr_config()['working_directory'], GL.Config.backup_needs, GL.Config.backup_ignores)
		tuple(map(server.execute, GL.Config.after_backup))
		send_message(source, 'Saving backup "{}"'.format(comment), log=True)
		backup.save(GL.Config.backup_path)
		send_message(source, 'Saved backup "{}"'.format(comment), log=True)

	if len(GL.Config.start_backup_trigger_info) > 0:
		begin_job()
		global game_saved_callback
		game_saved_callback = new_thread(lambda: (call(), after_job()))
	else:
		call()

@new_thread
@new_job('make full backup')
def command_makefull(source: MCDR.CommandSource, comment: str):
	server = source.get_server()

	def call():
		backup = Backup.create(BackupMode.FULL, comment,
			source.get_server().get_mcdr_config()['working_directory'], GL.Config.backup_needs, GL.Config.backup_ignores)
		tuple(map(server.execute, GL.Config.after_backup))
		send_message(source, 'Saving backup "{}"'.format(comment), log=True)
		backup.save(GL.Config.backup_path)
		send_message(source, 'Saved backup "{}"'.format(comment), log=True)

	begin_job()
	global game_saved_callback
	game_saved_callback = new_thread(lambda: (call(), after_job()))

	send_message(source, 'Making full backup "{}"'.format(comment), log=True)
	tuple(map(server.execute, GL.Config.befor_backup))


def command_confirm(source: MCDR.CommandSource):
	confirm_map.pop(source.player if source.is_player else '', (lambda s: send_message(s, '当前没有正在执行的操作'), 0))[0](source)

def command_abort(source: MCDR.CommandSource):
	c = confirm_map.pop(source.player if source.is_player else '', (0, 0))[1]
	if not c:
		c = confirm_map.pop(None, (0, lambda s: send_message(s, '当前没有正在执行的操作')))[1]
	c(source)

@new_thread
def command_config_load(source: MCDR.CommandSource):
	GL.Config = server.load_config_simple(target_class=GL.SMBConfig, source_to_reply=source)

@new_thread
def command_config_save(source: MCDR.CommandSource):
	GL.Config.save()
	send_message(source, 'Save config file SUCCESS')

confirm_map = {}

def __warp_call(call):
	def c(*b):
		return call(*b[:call.__code__.co_argcount])
	return c

def register_confirm(player: str, confirm_call, abort_call=lambda: 0):
	confirm_map[player] = (__warp_call(confirm_call), __warp_call(abort_call))
