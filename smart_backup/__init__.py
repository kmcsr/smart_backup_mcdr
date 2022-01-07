
import mcdreforged.api.all as MCDR
from .utils import *
from . import globals as GL
from . import commands as CMD
from . import api

def on_load(server: MCDR.PluginServerInterface, prev_module):
	if prev_module is None:
		log_info('Smart backup is on LOAD')
	else:
		log_info('Smart backup is on RELOAD')
	GL.init(server)
	CMD.register(server)

def on_unload(server: MCDR.PluginServerInterface):
	log_info('Smart backup is on UNLOAD')
	GL.destory()

def on_info(server: MCDR.ServerInterface, info: MCDR.Info):
  api.on_info(server, info)
