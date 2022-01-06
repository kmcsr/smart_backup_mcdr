
from threading import RLock, Condition
import functools

import mcdreforged.api.all as MCDR
from . import globals as GL

__all__ = [
	'new_thread', 'get_current_job', 'begin_job', 'after_job', 'new_job',
	'join_rtext', 'send_block_message', 'send_message', 'broadcast_message', 'log_info'
]

def new_thread(call):
	@MCDR.new_thread('smart_backup')
	def c(*args, **kwargs):
		return call(*args, **kwargs)
	return c

current_job = None
job_lock = Condition(RLock())

def get_current_job():
	with job_lock:
		return None if current_job is None else current_job[0]

def check_job(job: str):
	with job_lock:
		return current_job is None or current_job[0] == job

def begin_job(job: str = None, block=False):
	global current_job, job_lock
	if block or job is None or current_job is None or current_job[0] == job:
		with job_lock:
			while True:
				if current_job is None:
					assert job is not None
					current_job = [job, 1]
					return True
				if job is None or current_job[0] == job:
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
			with job_lock:
				if not check_job(job) and len(args) > 0 and isinstance(args[0], MCDR.CommandSource):
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

def join_rtext(*args, sep=' '):
	return MCDR.RTextList(args[0], *(MCDR.RTextList(sep, a) for a in args[1:]))

def send_block_message(source: MCDR.CommandSource, *args, sep='\n', log=False):
	if source is not None:
		t = join_rtext(GL.BIG_BLOCK_BEFOR, join_rtext(*args, sep=sep), GL.BIG_BLOCK_AFTER, sep='\n')
		source.reply(t)
		if log and not source.is_console:
			source.get_server().logger.info(t)


def send_message(source: MCDR.CommandSource, *args, sep=' ', prefix=GL.MSG_ID, log=False):
	if source is not None:
		t = join_rtext(prefix, *args, sep=sep)
		source.reply(t)
		if log and not source.is_console:
			source.get_server().logger.info(t)

def broadcast_message(*args, sep=' ', prefix=GL.MSG_ID):
	if GL.SERVER_INS is not None:
		GL.SERVER_INS.broadcast(join_rtext(prefix, *args, sep=sep))

def log_info(*args, sep=' ', prefix=GL.MSG_ID):
	if GL.SERVER_INS is not None:
		GL.SERVER_INS.logger.info(join_rtext(prefix, *args, sep=sep))
