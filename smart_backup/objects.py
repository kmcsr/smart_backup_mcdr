
import io
import os
import shutil
import time
import enum
import hashlib
import weakref
import queue
import json

__all__ = [
	'BackupNotFoundError',
	'ModifiedType', 'BackupMode',
	'BackupFile', 'BackupDir', 'Backup',
	'BackupIndex', 'BackupManager'
]

class BackupNotFoundError(FileNotFoundError):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class ModifiedType(int, enum.Enum):
	UNKNOWN = 0
	UPDATE = 1
	REMOVE = 2

class BackupMode(int, enum.Enum):
	FULL = 0
	INCREMENTAL = 1
	DIFFERENTIAL = 2

class BackupFile: pass
class BackupDir: pass
class Backup: pass
class BackupManager: pass

class BackupFile:
	def __init__(self, type_: ModifiedType, name: str, mode: int, data: bytes = None, path: str = None, offset: int = -1, safety: bool = False, hash_: bytes = None):
		self._type = type_
		self._name = name
		self._mode = mode
		self._data = data
		self._path = path
		self._offset = offset
		self._safety_data = safety
		self._hash = hash_

	@property
	def type(self):
		return self._type

	@property
	def name(self):
		return self._name

	@property
	def mode(self):
		return self._mode

	@property
	def hash(self):
		if self._mode == ModifiedType.REMOVE:
			return None
		if self._hash is None:
			with self.data_file as rd:
				self._hash = calchash(rd)
		return self._hash

	@property
	def safety_data(self):
		return self._safety_data

	@property
	def data(self):
		if self._type == ModifiedType.REMOVE:
			return None
		if self._data is not None:
			return self._data
		if self._path is not None:
			try:
				with open(self._path, 'rb', 8192) as fd:
					fd.seek(self._offset)
					return fd.read()
			except FileNotFoundError:
				raise
			except Exception as err:
				raise RuntimeError(f'Error when open {self._path}', err)

	@property
	def data_file(self):
		if self._type == ModifiedType.REMOVE:
			return None
		if self._data is not None:
			return io.BytesIO(self._data)
		if self._path is not None:
			try:
				fd = open(self._path, 'rb', 8192)
				fd.seek(self._offset)
				return fd
			except FileNotFoundError:
				raise
			except Exception as err:
				raise RuntimeError(f'Error when open {self._path}', err)

	def get(self, base, *path):
		return None

	@classmethod
	def create(cls, path: str, *pt, filterc=lambda *a, **b: True, prev: Backup = None):
		type_: ModifiedType
		name: str = pt[-1]
		mode: int
		hash_: bytes = None
		if os.path.exists(path):
			type_ = ModifiedType.UPDATE
			mode = os.stat(path).st_mode & 0o777
			if prev is not None:
				pref = prev.get(*pt)
				if isinstance(pref, cls) and mode == pref.mode:
					try:
						hash_ = calchash(open(path, 'rb', 8192))
						if hash_ == pref.hash:
							return None
					except FileNotFoundError:
						raise
					except Exception as err:
						raise RuntimeError(f'Error when read {path}', err)
		else:
			type_ = ModifiedType.REMOVE
			mode = 0
		return cls(type_=type_, name=name, mode=mode, path=path, offset=0, hash_=hash_)

	def restore(self, path: str):
		try:
			with open(path, 'wb', 8192) as wd, self.data_file as rd:
				writetofile(rd, wd)
		except Exception as err:
			raise RuntimeError(f'Error when restore {path}', err)

	def save(self, path: str):
		path = os.path.join(path, self._name + '.F')
		with open(path, 'wb', 8192) as fd:
			fd.write(self._type.to_bytes(1, byteorder='big'))
			if self._type != ModifiedType.REMOVE:
				fd.write(self._mode.to_bytes(2, byteorder='big'))
				fd.write(self.hash)
				with self.data_file as rd:
					writetofile(rd, fd)
		self._path, self._offset = path, 35

	@classmethod
	def load(cls, path: str, prev: BackupFile = None):
		type_: ModifiedType
		name: str = os.path.splitext(os.path.basename(path))[0]
		mode: int
		hash_: bytes = None
		with open(path, 'rb', 36) as fd:
			type_ = ModifiedType(int.from_bytes(fd.read(1), byteorder='big'))
			if type_ == ModifiedType.REMOVE:
				mode = 0
			else:
				mode = int.from_bytes(fd.read(2), byteorder='big')
				hash_ = fd.read(32)
		return cls(type_=type_, name=name, mode=mode, path=path, offset=35, safety=True, hash_=hash_)

class BackupDir:
	def __init__(self, type_: ModifiedType, name: str, mode: int, files: dict = None):
		self._type = type_
		self._name = name
		self._mode = mode
		self._files = dict((f.name, f) for f in files) if isinstance(files, (list, tuple, set)) else\
		 files.copy() if isinstance(files, dict) else {}

	@property
	def type(self):
		return self._type

	@property
	def name(self):
		return self._name

	@property
	def mode(self):
		return self._mode

	@property
	def files(self):
		return self._files.copy()

	def get(self, base, *path):
		f = self._files.get(base, None)
		if f is not None and len(path) > 0:
			f = f.get(*path)
		return f

	@classmethod
	def create(cls, path: str, *pt, filterc=lambda *a, **b: True, prev: Backup = None):
		type_: ModifiedType
		name: str = pt[-1]
		mode: int
		files: set = set()
		if os.path.isdir(path):
			mode = os.stat(path).st_mode & 0o777
			l = set(os.listdir(path))
			if prev is not None:
				l.update(prev.get_total_files(*pt))
			for n in filter(lambda a: filterc(os.path.join(*pt), a), l):
				f = os.path.join(path, n)
				files.add((BackupDir if os.path.isdir(f) else BackupFile if os.path.exists(f) else prev.get(*pt, n).__class__).\
					create(f, *pt, n, filterc=filterc, prev=prev))
			if prev is not None and len(files) == 0 and pt[-1] in prev.get_total_files(*pt[:-1]):
				return None
			type_ = ModifiedType.UPDATE
		else:
			type_ = ModifiedType.REMOVE
			mode = 0
		if None in files:
			files.remove(None)
		return cls(type_=type_, name=name, mode=mode, files=list(files))

	def save(self, path: str):
		path = os.path.join(path, self._name + '.D')
		if self._type == ModifiedType.REMOVE:
			with open(path, 'wb', 1) as fd:
				fd.write(self._type.to_bytes(1, byteorder='big'))
		else:
			os.mkdir(path)
			with open(os.path.join(path, '0'), 'wb', 3) as fd:
				fd.write(self._type.to_bytes(1, byteorder='big'))
				fd.write(self._mode.to_bytes(2, byteorder='big'))
			for f in self._files.values():
				f.save(path)

	@classmethod
	def load(cls, path: str):
		type_: ModifiedType
		name: str = os.path.splitext(os.path.basename(path))[0]
		mode: int
		files: list = []
		if os.path.isdir(path):
			with open(os.path.join(path, '0'), 'rb', 3) as fd:
				type_ = ModifiedType(int.from_bytes(fd.read(1), byteorder='big'))
				if type_ != ModifiedType.REMOVE:
					mode = int.from_bytes(fd.read(2), byteorder='big')
			if type_ != ModifiedType.REMOVE:
				for n in os.listdir(path):
					f = os.path.join(path, n)
					e = os.path.splitext(f)[1]
					if e == '.F':
						files.append(BackupFile.load(f))
					elif e == '.D':
						files.append(BackupDir.load(f))
		else:
			with open(path, 'rb', 1) as fd:
				type_ = ModifiedType(int.from_bytes(fd.read(1), byteorder='big'))
				assert type_ == ModifiedType.REMOVE
				mode = 0
		return cls(type_=type_, name=name, mode=mode, files=files)

class Backup:
	def __init__(self, mode: BackupMode, timestamp: int, comment: str, outdate: int, files: dict = None,
		*, safety: bool = False, manager: BackupManager = None, prev: [Backup, str] = None):
		self._manager = manager if manager is not None else None if prev is None else prev.manager
		assert self._manager is not None
		self._mode = mode
		self._timestamp = timestamp # unit: ms
		self._comment = comment
		self._outdate = outdate
		self._files = dict((f.name, f) for f in files) if isinstance(files, (list, tuple, set)) else files.copy() if isinstance(files, dict) else {}
		self._safety = safety
		self._prev = prev

	@property
	def manager(self):
		return self._manager

	@property
	def mode(self):
		return self._mode

	@property
	def id(self):
		return hex(self._timestamp)

	@property
	def timestamp(self):
		return self._timestamp / 1000

	@property
	def strftime(self):
		return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))

	@property
	def comment(self):
		return self._comment

	@property
	def outdate(self):
		return self._outdate

	@property
	def is_safety(self):
		return self._safety

	@property
	def prev(self):
		if self._mode == BackupMode.FULL:
			return None
		if isinstance(self._prev, Backup):
			return self._prev
		if isinstance(self._prev, str):
			self._prev = self._manager.load(self._prev)
			return self._prev
		raise TypeError(type(self._prev))

	@property
	def z_index(self):
		z: int = 0
		prev = self.prev
		while prev is not None:
			prev = prev.prev
			z += 1
		return z

	@property
	def files(self):
		return self._files.copy()

	def has_parent(self, pid):
		if self.prev is None:
			return False
		if self.prev.id == pid:
			return True
		return self.prev.has_parent(pid)

	def get_total_files(self, *path):
		files = self
		for f in path:
			files = files.files.get(f)
			if files is None:
				filel = set()
				break
		else:
			filel = set(files.files.keys())
		if self._mode != BackupMode.FULL:
			filel.update(self.prev.get_total_files(*path))
		return set(filter(lambda a: self.get(*path, a).type != ModifiedType.REMOVE, filel))

	def get(self, base, *path):
		f = self._files.get(base, None)
		if f is not None and len(path) > 0:
			f = f.get(*path)
		if f is None and self.prev is not None:
			return self.prev.get(base, *path)
		return f

	def restore(self, path: str, needs: list, ignores: list = []):
		if not self._safety:
			pth = os.path.join(self._basepath, hex(self._timestamp))
			if not os.path.exists(pth):
				self.save()
			self = self.__class__.load(self._basepath, cached=False)
		files = []
		que = queue.SimpleQueue()
		for f in self.get_total_files():
			que.put([f])
		while not que.empty():
			n = que.get_nowait()
			f = self.get(*n)
			files.append([os.path.join(path, *n), f])
			if isinstance(f, BackupDir):
				for m in self.get_total_files(*n):
					que.put([*n, m])
		filterc = filters(ignores)
		for n in needs:
			clear_dir(os.path.join(path, n), filterc)
		for p, f in files:
			if isinstance(f, BackupDir) and not os.path.exists(p):
				os.makedirs(p)
			elif isinstance(f, BackupFile):
				f.restore(p)

	def save(self):
		if not os.path.exists(self._manager.basepath):
			os.makedirs(self._manager.basepath)
		path = os.path.join(self._manager.basepath, hex(self._timestamp))
		os.mkdir(path)
		try:
			with open(os.path.join(path, '0'), 'wb', 8192) as fd:
				comment = self._comment.encode('utf8')
				fd.write(self._mode.to_bytes(1, byteorder='big'))
				fd.write(int(0 if self.prev is None else self.prev.timestamp * 1000).to_bytes(8, byteorder='big'))
				fd.write(int(self._outdate).to_bytes(8, byteorder='big'))
				fd.write(len(comment).to_bytes(2, byteorder='big'))
				fd.write(comment)
			for f in self._files.values():
				f.save(path)
		except:
			shutil.rmtree(path)
			raise
		else:
			self._manager.index.append(self)

	def remove(self):
		pred: list = self._manager.index.remove(self)
		for d in pred:
			shutil.rmtree(os.path.join(self._manager.basepath, d))

	def __hash__(self):
		return hash(hex(self.timestamp))

class BackupIndex:
	def __init__(self, *,
		last: str = None,
		ls: list = [],
		nodes: list = [],
		fulln: list = [],
		outdates: list = []):

		self._last = last
		self._list = ls
		self._nodes = nodes
		self._fulln = fulln
		self._outdates = outdates

	@property
	def last(self):
		return self._last

	@property
	def list(self):
		return self._list

	@property
	def nodes(self):
		return self._nodes

	@property
	def fulln(self):
		return self._fulln

	@property
	def outdates(self):
		return self._outdates

	def pop_outdated(self):
		if len(self._outdates) == 0:
			return None
		b = self._outdates[0]
		if b[1] <= time.time() // 60:
			self._outdates.pop(0)
			return b[0]
		return None

	def load(self, path: str):
		idx = os.path.join(path, 'index.json')
		index: dict = {}
		if os.path.exists(idx):
			with open(idx, 'r') as fd:
				index = json.load(fd)
		self._last = index.get('last', None)
		self._list = index.get('list', [])
		self._nodes = index.get('nodes', [])
		self._fulln = index.get('fulln', [])
		self._outdates = index.get('outdates', [])

	def save(self, path: str):
		with open(os.path.join(path, 'index.json'), 'w') as fd:
			json.dump({
				'last': self._last,
				'list': self._list,
				'nodes': self._nodes,
				'fulln': self._fulln,
				'outdates': self._outdates
			}, fd, separators=(',', ':'))

	def append(self, bk: Backup):
		bid: str = bk.id
		if bk.prev is None or self._last != bk.prev.id:
			self._nodes.append(bid)
			if bk.prev is None: # mode == BackupMode.FULL
				self._fulln.append(bid)
				if bk.outdate != 1:
					self._outdates = BackupIndex.insertOutdate(self._outdates, bk)
		self._last = bid
		self._list.append(bid)

	@staticmethod
	def insertOutdate(outdates: list, bk: Backup):
		manager = bk.manager
		i: int
		for i, b in enumerate(outdates):
			if b[1] > bk.outdate:
				break
		else:
			outdates.append([bk.id, bk.outdate])
			return outdates
		outdates.insert(i, [bk.id, bk.outdate])
		print('OUTDATES:', outdates)
		return outdates

	def remove(self, bk: Backup):
		pr: Backup = bk
		i: int
		while True:
			try:
				i = self._nodes.index(pr.id)
			except ValueError as e:
				pr = pr.prev
			else:
				break
		s = self._list.index(bk.id)
		while True:
			i += 1
			if i >= len(self._nodes):
				e = len(self._list)
				break
			nid = self._nodes[i]
			if not bk.manager.load(nid).has_parent(bk.id):
				e = self._list.index(nid)
				break
		if e == len(self._list):
			self._last = self._list[s - 1] if s > 0 else None
		lst = self._list[s:e]
		del self._list[s:e]
		self._nodes = [i for i in self._nodes if i not in lst]
		self._fulln = [i for i in self._fulln if i not in lst]
		self._outdates = [i for i in self._outdates if i not in lst]
		return lst

class BackupManager:
	def __init__(self, basepath: str):
		self.__cache = weakref.WeakValueDictionary()
		self.__basepath = basepath
		self.__index = BackupIndex()

		self._loadcfg()

	@property
	def basepath(self):
		return self.__basepath

	@property
	def index(self):
		return self.__index

	def _loadcfg(self):
		self.__index.load(self.__basepath)

	def savecfg(self):
		if not os.path.exists(self.__basepath):
			os.makedirs(self.__basepath)
			self.__index = BackupIndex()
		self.__index.save(self.__basepath)

	def listID(self):
		# return sorted(map(lambda a: int(a, 16), filter(lambda a: a.startswith('0x'), os.listdir(self.basepath))))
		return self.index.list

	def create(self, mode: BackupMode, comment: str, outdate: int, base: str, needs: list, ignores: list = [], saved: bool = True):
		prev: Backup = None
		if mode != BackupMode.FULL:
			if self.index.last is None:
				mode = BackupMode.FULL
			else:
				prev = self.load(self.index.last)

		timestamp: int = int(time.time() * 1000)
		files: set = set()
		l = set(os.listdir(base))
		if prev is not None:
			if mode == BackupMode.DIFFERENTIAL:
				while prev.mode != BackupMode.FULL:
					prev = prev.prev
			l.update(prev.get_total_files())
		filterc = filters(ignores)
		for n in filter(lambda a: a in needs, l):
			m = os.path.join(base, n)
			files.add((BackupDir if os.path.isdir(m) else BackupFile if os.path.exists(m) else prev.get(n).__class__).\
				create(m, n, filterc=filterc, prev=prev))
		if None in files:
			files.remove(None)
		bk = Backup(mode=mode, timestamp=timestamp, comment=comment, outdate=outdate, files=list(files), manager=self, prev=prev)
		if saved:
			bk.save()
		return bk

	def load(self, bid: str, cached: bool = True):
		if bid in self.__cache:
			if cached:
				return self.__cache[bid]
			else:
				self.__cache.pop(bid)

		path: str = os.path.join(self.__basepath, bid)
		if not os.path.exists(path):
			raise BackupNotFoundError('Backup id {0} not found in "{1}"'.format(bid, self.__basepath))
		mode: BackupMode
		timestamp: int = int(bid, 16)
		comment: str
		outdate: int
		files: list = []
		previd: int
		prev: Backup = None
		with open(os.path.join(path, '0'), 'rb', 8192) as fd:
			mode = BackupMode(int.from_bytes(fd.read(1), byteorder='big'))
			previd = int.from_bytes(fd.read(8), byteorder='big')
			outdate = int.from_bytes(fd.read(8), byteorder='big')
			comment = fd.read(int.from_bytes(fd.read(2), byteorder='big')).decode('utf8')
		if previd != 0:
			assert previd != timestamp
		for n in os.listdir(path):
			f = os.path.join(path, n)
			e = os.path.splitext(f)[1]
			if e == '.F':
				files.append(BackupFile.load(f))
			elif e == '.D':
				files.append(BackupDir.load(f))

		bk = Backup(mode=mode, timestamp=timestamp, comment=comment, outdate=outdate, files=files, safety=True, manager=self, prev=None if previd == 0 else hex(previd))
		self.__cache[bid] = bk
		return bk

	def list(self, limit: int = -1):
		if not os.path.exists(self.basepath):
			return list()
		ids: list = self.listID()
		if limit > 0:
			ids = ids[-limit:]
		return [self.load(i) for i in ids]

	def get_last(self):
		if not os.path.exists(self.basepath) or self.index.last is None:
			return None
		return self.load(self.index.last)


def _filter(ignore: str):
	if len(ignore) == 0:
		return lambda *a, **b: True
	f = []
	if ignore[0] == '/':
		dr, ignore = os.path.split(ignore[1:])
		f.append(lambda path, base: path == dr)
	elif '/' in ignore:
		dr, ignore = os.path.split(ignore[1:])
		f.append(lambda path, base: path.endswith(dr))
	if ignore[0] == '*':
		ignore = ignore[1:]
		f.append(lambda path, base: base.endswith(ignore))
	else:
		f.append(lambda path, base: base == ignore)
	def c(path: str, base: str):
		for x in f:
			if not x(path, base):
				return False
		return True
	return c

def filters(ignores: list):
	ignorec = []
	for i, s in enumerate(ignores):
		ignorec.append(_filter(s))
	def call(path: str, base: str):
		for c in ignorec:
			if c(path, base):
				return False
		return True
	return call

def writetofile(src, dst):
	if isinstance(src, (bytes, str)):
		dst.write(src)
		return
	if isinstance(src, io.IOBase):
		while True:
			b = src.read(8192)
			if not b:
				break
			dst.write(b)
		return
	raise TypeError(type(data))

def calchash(data):
	if isinstance(data, bytes):
		return hashlib.sha256(data).digest()
	if isinstance(data, io.IOBase):
		h = hashlib.sha256()
		while True:
			d = data.read(8192)
			if not d:
				break
			h.update(d)
		return h.digest()
	raise TypeError(type(data))

def clear_dir(path: str, filterc):
	if not os.path.exists(path):
		return
	if not os.path.isdir(path):
		os.remove(path)
		return
	que = queue.SimpleQueue()
	dirs = {}
	for f in os.listdir(path):
		que.put(os.path.join(path, f))
	while not que.empty():
		f = que.get_nowait()
		if not filterc(f, os.path.basename(f)):
			continue
		if os.path.isdir(f):
			ls = os.listdir(f)
			if len(ls) == 0:
				os.rmdir(f)
			else:
				dirs[f] = len(ls)
				for i in ls:
					que.put(os.path.join(f, i))
		else:
			os.remove(f)
			d = os.path.dirname(f)
			if d in dirs:
				dirs[d] -= 1
				if dirs[d] == 0:
					dirs.pop(d)
					os.rmdir(d)
