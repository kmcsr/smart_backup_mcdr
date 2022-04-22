
import os
from threading import RLock, Condition, Timer
import functools

import mcdreforged.api.all as MCDR
from . import globals as GL

__all__ = [
	'new_thread', 'get_current_job', '_clear_job', 'after_job_wrapper', 'ping_job', 'after_job', 'next_job_call', 'new_job', 'new_timer',
	'new_command', 'join_rtext', 'send_block_message', 'send_message', 'broadcast_message', 'log_info',
	'get_total_size', 'format_size'
]

def new_thread(call):
	@MCDR.new_thread('smart_backup')
	@functools.wraps(call)
	def c(*args, **kwargs):
		return call(*args, **kwargs)
	return c

current_job = None
job_lock = Condition(RLock())

def get_current_job():
	with job_lock:
		return None if current_job is None else current_job[0]

def check_job():
	with job_lock:
		return current_job is None

def _clear_job():
	global current_job
	current_job = None

def begin_job(job: str, block=False):
	global current_job, job_lock
	with job_lock:
		while True:
			if current_job is None or current_job is False:
				current_job = [job, 1]
				return True
			if not block:
				break
			job_lock.wait()
	return False

def ping_job():
	global current_job, job_lock
	with job_lock:
		current_job[1] += 1

def after_job():
	global current_job, job_lock
	with job_lock:
		if current_job is not None:
			current_job[1] -= 1
			if current_job[1] == 0:
				current_job = None
				job_lock.notify()

def after_job_wrapper(call):
	@functools.wraps(call)
	def c(*args, **kwargs):
		try:
			return call(*args, **kwargs)
		finally:
			after_job()
	return c

def next_job_call(call, *args, **kwargs):
	global current_job, job_lock
	with job_lock:
		assert current_job is not None and current_job is not False
		current_job = False
	return call(*args, **kwargs)

def new_job(job: str, block=False):
	def w(call):
		@functools.wraps(call)
		def c(*args, **kwargs):
			with job_lock:
				if current_job is not None and current_job is not False and not block:
					if len(args) > 0 and isinstance(args[0], MCDR.CommandSource):
						send_message(args[0], MCDR.RText('In progress {} now'.format(current_job[0]), color=MCDR.RColor.red))
					else:
						log_info(MCDR.RText('In progress {0} now, cannot do {1}'.format(current_job[0], job), color=MCDR.RColor.red))
					return None
				begin_job(job, block=True)
			try:
				return call(*args, **kwargs)
			finally:
				after_job()
		return c
	return w

def new_timer(interval, call, args: list=None, kwargs: dict=None, daemon: bool=True, name: str='smart_backup_timer'):
	tm = Timer(interval, call, args=args, kwargs=kwargs)
	tm.name = name
	tm.daemon = daemon
	tm.start()
	return tm

def new_command(cmd: str, text=None, **kwargs):
	if text is None:
		text = cmd
	if 'color' not in kwargs:
		kwargs['color'] = MCDR.RColor.yellow
	if 'styles' not in kwargs:
		kwargs['styles'] = MCDR.RStyle.underlined
	return MCDR.RText(text, **kwargs).c(MCDR.RAction.run_command, cmd).h(cmd)

def join_rtext(*args, sep=' '):
	if len(args) == 0:
		return MCDR.RTextList()
	if len(args) == 1:
		return MCDR.RTextList(args[0])
	return MCDR.RTextList(args[0], *(MCDR.RTextList(sep, a) for a in args[1:]))

def send_block_message(source: MCDR.CommandSource, *args, sep='\n', log=False):
	if source is not None:
		t = join_rtext(GL.BIG_BLOCK_BEFOR, join_rtext(*args, sep=sep), GL.BIG_BLOCK_AFTER, sep='\n')
		source.reply(t)
		if log and source.is_player:
			source.get_server().logger.info(t)

def send_message(source: MCDR.CommandSource, *args, sep=' ', prefix=GL.MSG_ID, log=False):
	if source is not None:
		t = join_rtext(prefix, *args, sep=sep)
		source.reply(t)
		if log and source.is_player:
			source.get_server().logger.info(t)

def broadcast_message(*args, sep=' ', prefix=GL.MSG_ID):
	if GL.SERVER_INS is not None:
		GL.SERVER_INS.broadcast(join_rtext(prefix, *args, sep=sep))

def log_info(*args, sep=' ', prefix=GL.MSG_ID):
	if GL.SERVER_INS is not None:
		GL.SERVER_INS.logger.info(join_rtext(prefix, *args, sep=sep))

def get_total_size(path: str):
	size = 0
	for root, _, files in os.walk(path):
		for f in files:
			f = os.path.join(root, f)
			size += os.stat(f).st_size
	return size

__bt_units = ('B', 'KB', 'MB', 'GB', 'TB', 'PB')

def format_size(size: int):
	sz: float = float(size)
	ut: str = __bt_units[0]
	for u in __bt_units[1:]:
		if sz >= 1000:
			sz /= 1024
			ut = u
	return '{0:.2f}{1}'.format(sz, ut)
