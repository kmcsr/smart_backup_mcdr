
import os
import time
import enum
import weakref
import queue

__all__ = [
	'ModifiedType', 'BackupMode', 'BackupFile', 'BackupDir', 'Backup'
]

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

class BackupFile:
	def __init__(self, type_: ModifiedType, name: str, mode: int, data: bytes = None, path: str = None, offset: int = -1):
		self._type = type_
		self._name = name
		self._mode = mode
		self._data = data
		self._path = path
		self._offset = offset

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
	def data(self):
		if self._type == ModifiedType.REMOVE:
			return None
		if self._data is not None:
			return self._data
		if self._path is not None:
			with open(self._path, 'rb') as fd:
				fd.seek(self._offset)
				return fd.read()

	def get(self, base, *path):
		return None

	@classmethod
	def create(cls, path: str, *pt, filterc=None, prev: Backup = None):
		type_: ModifiedType
		name: str = pt[-1]
		mode: int
		if os.path.exists(path):
			mode = os.stat(path).st_mode & 0o777
			with open(path, 'rb') as fd:
				data = fd.read()
			if prev is not None:
				pref = prev.get(*pt)
				if isinstance(pref, cls) and mode == pref.mode and data == pref.data:
					return None
			type_ = ModifiedType.UPDATE
		else:
			type_ = ModifiedType.REMOVE
			mode = 0
		return cls(type_=type_, name=name, mode=mode, path=path, offset=0)

	def restore(self, path: str):
		with open(path, 'wb') as wd:
			if self._data is not None:
				wd.write(self._data)
			else:
				with open(self._path, 'rb') as rd:
					rd.seek(self._offset)
					while True:
						b = rd.read(8192)
						if not b:
							break
						wd.write(b)

	def save(self, path: str):
		path = os.path.join(path, self._name + '.F')
		with open(path, 'wb') as fd:
			fd.write(self._type.to_bytes(1, byteorder='big'))
			if self._type != ModifiedType.REMOVE:
				fd.write(self._mode.to_bytes(2, byteorder='big'))
				data = self.data
				# TODO compress data
				fd.write(data)
				self._path, self._offset = path, 3

	@classmethod
	def load(cls, path: str, prev: BackupFile = None):
		type_: ModifiedType
		name: str = os.path.splitext(os.path.basename(path))[0]
		mode: int
		with open(path, 'rb') as fd:
			type_ = ModifiedType(int.from_bytes(fd.read(1), byteorder='big'))
			if type_ == ModifiedType.REMOVE:
				mode = 0
			else:
				mode = int.from_bytes(fd.read(2), byteorder='big')
		return cls(type_=type_, name=name, mode=mode, path=path, offset=3)

class BackupDir:
	def __init__(self, type_: ModifiedType, name: str, mode: int, files: dict = None):
		self._type = type_
		self._name = name
		self._mode = mode
		self._files = dict((f.name, f) for f in files) if isinstance(files, (list, tuple, set)) else files.copy() if isinstance(files, dict) else {}

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
	def create(cls, path: str, *pt, filterc=lambda _: True, prev: Backup = None):
		type_: ModifiedType
		name: str = pt[-1]
		mode: int
		files: list = []
		if os.path.isdir(path):
			mode = os.stat(path).st_mode & 0o777
			l = set(os.listdir(path))
			if prev is not None:
				l.update(prev.get_total_files(*pt))
			for n in filter(filterc, l):
				f = os.path.join(path, n)
				o = (BackupDir if os.path.isdir(f) else BackupFile if os.path.exists(f) else prev.get(*pt, n).__class__).create(f, *pt, n, prev=prev)
				if o is not None:
					files.append(o)
			if prev is not None and len(files) == 0 and pt[-1] in prev.get_total_files(*pt[:-1]):
				return None
			type_ = ModifiedType.UPDATE
		else:
			type_ = ModifiedType.REMOVE
			mode = 0
		return cls(type_=type_, name=name, mode=mode, files=files)

	def save(self, path: str):
		path = os.path.join(path, self._name + '.D')
		if self._type == ModifiedType.REMOVE:
			with open(path, 'wb') as fd:
				fd.write(self._type.to_bytes(1, byteorder='big'))
		else:
			os.mkdir(path)
			with open(os.path.join(path, '0'), 'wb') as fd:
				fd.write(
					self._type.to_bytes(1, byteorder='big') +
					self._mode.to_bytes(2, byteorder='big'))
			for f in self._files.values():
				f.save(path)

	@classmethod
	def load(cls, path: str):
		type_: ModifiedType
		name: str = os.path.splitext(os.path.basename(path))[0]
		mode: int
		files: list = []
		if os.path.isdir(path):
			with open(os.path.join(path, '0'), 'rb') as fd:
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
			with open(path, 'rb') as fd:
				type_ = ModifiedType(int.from_bytes(fd.read(1), byteorder='big'))
				assert type_ == ModifiedType.REMOVE
				mode = 0
		return cls(type_=type_, name=name, mode=mode, files=files)

class Backup:
	_cache = weakref.WeakValueDictionary()

	def __init__(self, mode: BackupMode, timestamp: int, comment: str, files: dict = None, prev: Backup = None):
		self._mode = mode
		self._timestamp = timestamp # unit: ms
		self._comment = comment
		self._files = dict((f.name, f) for f in files) if isinstance(files, (list, tuple, set)) else files.copy() if isinstance(files, dict) else {}
		self._prev = prev

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
	def prev(self):
		return self._prev

	@property
	def z_index(self):
		z = 0
		prev = self._prev
		while prev is not None:
			prev = prev.prev
			z += 1
		return z

	@property
	def files(self):
		return self._files.copy()

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
			filel.update(self._prev.get_total_files(*path))
		return set(filter(lambda a: self.get(*path, a).type != ModifiedType.REMOVE, filel))

	def get(self, base, *path):
		f = self._files.get(base, None)
		if f is not None and len(path) > 0:
			f = f.get(*path)
		if f is None and self._prev is not None:
			return self._prev.get(base, *path)
		return f

	@classmethod
	def create(cls, mode: BackupMode, comment: str, base: str, needs: list, ignores: list = [], prev: Backup = None):
		timestamp: int = int(time.time() * 1000)
		files: list = []
		l = set(os.listdir(base))
		if mode == BackupMode.FULL:
			if prev is not None:
				prev = None
		else:
			assert prev is not None
			if mode == BackupMode.DIFFERENTIAL:
				while prev.mode != BackupMode.FULL:
					prev = prev.prev
			l.update(prev.get_total_files())
		filterc = filters(ignores)
		for n in filter(lambda a: a in needs, l):
			m = os.path.join(base, n)
			o = (BackupDir if os.path.isdir(m) else BackupFile if os.path.exists(m) else prev.get(n).__class__).create(m, n, filterc=filterc, prev=prev)
			if o is not None:
				files.append(o)
		return cls(mode=mode, timestamp=timestamp, comment=comment, files=files, prev=prev)

	def restore(self, path: str, needs: list, ignores: list = []):
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

	def save(self, path: str):
		if not os.path.exists(path):
			os.makedirs(path)
		path = os.path.join(path, hex(self._timestamp))
		os.mkdir(path)
		with open(os.path.join(path, '0'), 'wb') as fd:
			comment = self._comment.encode('utf8')
			fd.write(
				self._mode.to_bytes(1, byteorder='big') +
				int(0 if self._prev is None else self._prev.timestamp * 1000).to_bytes(8, byteorder='big') +
				len(comment).to_bytes(2, byteorder='big'))
			fd.write(comment)
		for f in self._files.values():
			f.save(path)

	@classmethod
	def load(cls, path: str):
		rpath = os.path.realpath(path)
		if rpath in cls._cache:
			return cls._cache[rpath]
		if not os.path.exists(path):
			return None
		mode: BackupMode
		timestamp: int = int(os.path.basename(path), 16)
		comment: str
		files: list = []
		prev: Backup = None
		with open(os.path.join(path, '0'), 'rb') as fd:
			mode = BackupMode(int.from_bytes(fd.read(1), byteorder='big'))
			previd = int.from_bytes(fd.read(8), byteorder='big')
			if previd != 0:
				assert previd != timestamp
				prev = cls.load(os.path.join(os.path.dirname(path), hex(previd)))
			comment = fd.read(int.from_bytes(fd.read(2), byteorder='big')).decode('utf8')
		for n in os.listdir(path):
			f = os.path.join(path, n)
			e = os.path.splitext(f)[1]
			if e == '.F':
				files.append(BackupFile.load(f))
			elif e == '.D':
				files.append(BackupDir.load(f))
		obj = cls(mode=mode, timestamp=timestamp, comment=comment, files=files, prev=prev)
		cls._cache[rpath] = obj
		return obj

	def __hash__(self):
		return hash(hex(self.timestamp))

	@staticmethod
	def list(path: str, limit: int = -1):
		if not os.path.exists(path):
			return list()
		ids = sorted(map(lambda a: int(a, 16), filter(lambda a: a.startswith('0x'), os.listdir(path))))
		if limit > 0:
			ids = ids[-limit:]
		return [Backup.load(os.path.join(path, hex(i))) for i in ids]

	@staticmethod
	def get_last(path: str):
		if not os.path.exists(path):
			return None
		ids = sorted(map(lambda a: int(a, 16), filter(lambda a: a.startswith('0x'), os.listdir(path))))
		if len(ids) == 0:
			return None
		return Backup.load(os.path.join(path, hex(ids[-1])))


def _filter(ignore: str):
	if len(ignore) == 0:
		return lambda *a, **b: 1
	if ignore[0] == '/':
		ignore = ignore[1:]
		def call(path: str):
			return 1 if path == ignore else 0
	else:
		def call(path: str):
			return 1 if os.path.basename(path) == ignore else 0
	return call

def filters(ignores: list):
	ignorec = []
	for i, s in enumerate(ignores):
		ignorec.append(_filter(s))

	def call(path: str):
		for c in ignorec:
			s = c(path)
			if s == 1:
				return False
		return True
	return call

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
		if not filterc(os.path.basename(f)):
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
