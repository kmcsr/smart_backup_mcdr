
import os
from threading import RLock, Condition, Timer
import functools

import mcdreforged.api.all as MCDR
from . import globals as GL

__all__ = [
	'new_thread', 'get_current_job', 'begin_job', 'after_job', 'new_job', 'next_job', 'new_timer',
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

def begin_job(job: str = None, block=False):
	global current_job, job_lock
	if block or job is None or current_job is None:
		with job_lock:
			while True:
				if current_job is None:
					assert job is not None
					current_job = [job, 1]
					return True
				if job is None:
					current_job[1] += 1
					return True
				if not block:
					break
				job_lock.wait()
	return False

def after_job():
	global current_job, job_lock
	with job_lock:
		assert current_job is not None
		current_job[1] -= 1
		if current_job[1] == 0:
			current_job = None
			job_lock.notify()

def new_job(job: str):
	def w(call):
		@functools.wraps(call)
		def c(*args, **kwargs):
			if job_lock._is_owned():
				return call(*args, **kwargs)
			with job_lock:
				if not check_job() and len(args) > 0 and isinstance(args[0], MCDR.CommandSource):
					send_message(args[0], MCDR.RText('In progress {} now'.format(current_job[0]), color=MCDR.RColor.red))
					return None
				else:
					begin_job(job, block=True)
			try:
				return call(*args, **kwargs)
			finally:
				after_job()
		return c
	return w

def next_job(call):
	with job_lock:
		call()

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
	return MCDR.RText(text, **kwargs).c(MCDR.RAction.run_command, cmd)

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

def format_size(size: int):
	sz: float = float(size)
	ut: str = 'B'
	if sz >= 1000:
		sz /= 1024
		ut = 'KB'
	if sz >= 1000:
		sz /= 1024
		ut = 'MB'
	if sz >= 1000:
		sz /= 1024
		ut = 'GB'
	if sz >= 1000:
		sz /= 1024
		ut = 'TB'
	if sz >= 1000:
		sz /= 1024
		ut = 'PB'
	return '{0:.2f}{1}'.format(sz, ut)
