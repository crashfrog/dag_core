
import json
import re
from collections import defaultdict
from fnmatch import fnmatch
import traceback
from functools import wraps
import sys

STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_FINISH = 'completed'
STATUS_FATAL = 'fatal error'
STATUS_IGNORED = 'ignored by condition'


_root = None


class FatalException(Exception):
	pass
	
class CycleException(Exception):
	pass
	
def fix_stacktrace(func):
	@wraps(func)
	def manipulate_stacktrace_on_exception(*args, **kwargs):
		"Try to reduce the confusion of exception stacktraces that arise from an explict raise."
		try:
			return func(*args, **kwargs)
		except:
			#edit the stacktrace to remove the last two frames"
			exc_type, exc_value, exc_traceback = sys.exc_info()
			raise exc_type, exc_value, exc_traceback
	return manipulate_stacktrace_on_exception
	
	
def scan_modules(task):
	return_set = set()
	if hasattr(task, 'modules'):
		return_set.update(task.modules)
	[return_set.update(scan_modules(c)) for c in task.__children__]
	return return_set
	
	
_slugify_strip_re = re.compile(r'[^\w\s-]')
_slugify_hyphenate_re = re.compile(r'[-\s]+')
def _slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    
    From Django's "django/template/defaultfilters.py".
    """
    import unicodedata
    if not isinstance(value, unicode):
        value = unicode(value)
    
        
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(_slugify_strip_re.sub('', value).strip().lower())
    return _slugify_hyphenate_re.sub('-', value).strip('-')
    
class Relationship(object):
	def __init__(self, parent_task, child_task):
		self.parent_task = parent_task
		self.child_task = child_task
		
	def __getattr__(self, attr):
		"override so you can do 't = Task().follows(s)' and have t act like a task"
		try:
			return super(Relationship, self).__getattr__(attr)
		except AttributeError:
			return child_task.__getattr__(attr)
		
	def when(self, conditional_func_or_field, v=None):
		if hasattr(conditional_func_or_field, '__call__'):
			conditional_func = conditional_func_or_field
		else:
			if v is None:
				raise ValueError('arguments to "when" must be a callable, or a keyword and a value.')
			c = conditional_func_or_field
			conditional_func = lambda **r: (c in r and r[c] == v)
		self.parent_task.__conditionals__[self.child_task] = conditional_func
		return self
		
	def follows(self, parent_task):
		return self.child_task.follows(parent_task)
		
	def _or(self, cond_func):
		try:
			first_cond = self.parent_task.__conditionals__[self.child_task]
			disjunction = lambda **k: first_cond(**k) or second_cond(**k)
			self.parent_task.__conditionals__[self.child_task] = disjunction
			return self
		except KeyError:
			return SyntaxError("'or' called on undeclared conditional (use 'when()' first.)")
		
	def _and(self, second_cond):
		try:
			first_cond = self.parent_task.__conditionals__[self.child_task]
			conjunction = lambda **k: first_cond(**k) and second_cond(**k)
			self.parent_task.__conditionals__[self.child_task] = conjunction
			return self
		except KeyError:
			return SyntaxError("'and' called on undeclared conditional (use 'when()' first.)")
			
	def _else(self, alt_child):
		try:
			alt_child.follows(self.parent_task)
			cond = self.parent_task.__conditionals__[self.child_task]
			negation = lambda **k: not cond(**k)
			self.parent_task.__conditionals__[alt_child] = negation
		except KeyError:
			return SyntaxError("'else' called on undeclared conditional (use 'when()' first.)")

class AbstractTask(object):
	
	def __init__(self,
				 name,
				 file_filter = None,
				 **kwargs):
		#These fields get pickled in the Job state
		self.name = name
		self.slug = _slugify(name)
		self.params = kwargs
		self.termargs = []
		self.status = STATUS_PENDING
		self.stdin = ""
		self.input_files = []
		self.command_log = []
		
		#These do not
		self.__children__ = set()
		self.__parents__ = set()
		self.__conditionals__ = dict()
		self.__touch__ = False
		self.__preprocessors__ = []
		self.__postprocessors__ = []
		self.__file_filter = lambda f: True #by default, all tasks pass on all input files
		
		
		global _root
		if _root == None:
			_root = self
			
		if file_filter:
			if isinstance(file_filter, str):
				self.__file_filter = lambda f: fnmatch(f, file_filter)
				self.file_filter = file_filter
			elif hasattr(file_filter, '__iter__'):
				self.__file_filter = lambda f: any([fnmatch(f, fil) for fil in file_filter])
				self.file_filter = str(file_filter)
			elif callable(file_filter):
				self.__file_filter = file_filter
				self.file_filter = str(file_filter)
			else:
				raise ValueError("file_filter must be a string glob pattern ('*.csv'), iterable of globs, or callable")
			
	def start(self, **kwargs):
		"Subclasses should override start to implement features."
		#AbstractTask should immediately complete, since it does nothing.
		self.status = STATUS_FINISH
		return None
	
	@fix_stacktrace	
	def follows(self, parent):
		if not isinstance(parent, AbstractTask):
			if isinstance(parent, Relationship):
				return self.follows(parent.parent_task)
			raise ValueError("Graph relationships must be between Task objects; supplied type was '{}'.".format(type(parent)))
				
		#check for introduction of a cycle
		self.raise_if_child(parent)
		parent.__children__.add(self)
		self.__parents__.add(parent)
		global _root
		if _root == self:
			_root = parent
		return Relationship(parent, self)
		
	@fix_stacktrace	
	def raise_if_child(self, potential_parent):
		if potential_parent in self.__children__:
			raise CycleException("Graph must remain acyclic.")
		[c.raise_if_child(potential_parent) for c in self.__children__]
		
		
	def __bind__(self, 
		
		
# 	def __start__(self, *a, **kw):
# 		self.stdout(self.stdin)
# 		args = list(a)
# 		self.params.update(kw)
# 		kwargs = self.__bind__(self.termargs)
# 		return self.start(**kwargs)
# 		
# 	def __bind__(self, termargs):
# 		kwargs = defaultdict(list)
# 		[kwargs[k].append(v) for k, v in self.params.items()]
# 		for arg in self.termargs:
# 			try:
# 				key, value = arg
# 				kwargs[key].append(value)
# 			except ValueError:
# 				args.append(arg)
# 		[f(**kwargs) for f in self.__preprocessors__]
# 		return kwargs
# 		
# 	def __finalize__(self, record, stdout, file_list, *args, **kwargs):
# 		self.status = STATUS_FINISH
# 		self.stdin += stdout
# 		for c in self.__children__:
# 			if not self.__conditionals__.get(c, lambda **r: True)(**dict(self.__dict__, **kwargs)):
# 				c.status = STATUS_IGNORED
# 			else:
# 				c.__preload_files__(file_list)
# 		[f(self, *args, **kwargs) for f in self.__postprocessors__]
# 		#self.termargs.extend(self.finalize(*args, **kwargs))
# 		[c.termargs.extend(self.termargs) for c in self.__children__]
# 		return self
		
	def finalize(self, termargs):
		"Subclasses should override finalize to modify what termargs get passed on to children."
		return termargs
		
	def __preload_files__(self, file_list):
		for path in file_list:
			if self.__file_filter(path):
				self.input_files.append(path)

		
	def stdout(self, stdoutput):
		pass
		
	def prep(self, prep_func):
		self.__preprocessors__.append(prep_func)
		return prep_func
		
	def post(self, post_func):
		self.__postprocessors__.append(post_func)
		return post_func
		
	def __str__(self):
		if self.__children__:
			return "Task {0}, followed by: {1}".format(self.name, ', '.join([c.name for c in self.__children__]))
		else:
			return "Task {0}".format(self.name)
			
	def __repr__(self):
		return self.slug
		
	def __untouch__(self):
		self.__touch__ = False
		[c.__untouch__() for c in self.__children__]
		return self
		
	def __to_dict__(self):
		s = dict()
		if not self.__touch__:
			s.update(self.__dict__)
			for key in s.keys():
				if '__' in key:
					del s[key]
			self.__touch__ = True
		else:
			s['slug'] = self.slug
		s['~children'] = [c.__to_dict__() for c in self.__children__]
		return s
		
	def __from_dict__(self, s):
		for key, value in s.items():
			if '~' not in key:
				setattr(self, key, value)
		for child in self.__children__:
			for struct in s['~children']:
				if struct['slug'] == child.name:
					child.__from_dict__(struct)
		return self
		
		
	def __serialize__(self):
		self.__untouch__()
		s = self.__to_dict__()
		return json.dumps(s, ensure_ascii=True, indent=2, separators=(',', ': '), sort_keys=True)
		
		
	def __deserialize__(self, ser):
		struct = json.loads(ser)
		self.__from_dict__(struct)
		return self
	
	@fix_stacktrace	
	def __get_next__(self, nexts=None):
		if nexts is None:
			nexts = set()
		if self.status in STATUS_FATAL:
			raise FatalException("Task {s.name} (job id:{s.job_id}) is in a terminal failure state.".format(s=self))
		if (self.status in STATUS_PENDING or self.status in STATUS_RUNNING) and all(map(lambda p: p.status in STATUS_FINISH, self.__parents__)):
			nexts.add(self)
		if self.status in STATUS_FINISH:
			[c.__get_next__(nexts) for c in self.__children__]
		return nexts
		
	def __setRunning__(self, job_id):
		self.job_id = job_id
		self.status = STATUS_RUNNING
		return self
		
	def __setFatal__(self, error_condition=None):
		self.status = STATUS_FATAL
		self.term_error = "{};{}".format(type(error_condition), error_condition)
		return self
		
	def is_root(self):
		global _root
		_root = self
		self.__scan_modules__ = lambda: scan_modules(self)
		self.__getCommandLog__ = lambda: self.command_list
		return self
		
	def __getJobIds__(self):
		if hasattr(self, "job_id"):
			return self.job_id
			
	def __getSlug__(self):
		return self.slug
		
	def test(self):
		self.__test__()
		[c.test() for c in self.__children__]
		
	def __test__(self):
		print(self.name)


class ClusterTask(AbstractTask):
	
	def __init__(self,
				 name,
				 run,
				 modules=[],
				 **kwargs):
		super(ClusterTask, self).__init__(name, **kwargs)
		self.run_command = run
		self.job_id = None
		self.modules = modules
	
	
	def start(self, **kwargs):
		params = dict()
		params.update(self.params)
		params.update(kwargs)
		mods = '; '.join(['module load {}'.format(m) for m in self.modules])
		if '{input}' in self.run_command:
			command_set = set(['; '.join((mods, self.run_command.format(input=i, **params).replace('\n',' ').replace('\t',' '))) for i in self.input_files])
		else:
			command_set = set(('; '.join((mods, self.run_command.format(**params).replace('\n',' ').replace('\t',' ')))), ) #one-element set
		_root.command_log.extend(command_set)
		return command_set
		
	def __test__(self):
		print('{} ["{}"]'.format(self.name, ' '.join(self.run_command.split()))) #self.run_command.replace('\t',' ').replace('\n',' ')))

		
class NullTask(AbstractTask):
	 
	def __init__(self):
		super(NullTask, self).__init__("Null task", None)
		del self.params
	 	
	@property
	def status(self):
		return STATUS_FINISH
	 	
	@status.setter
	def status(self, value):
		pass
		
	@status.deleter
	def status(self):
		pass
		

if __name__ == '__main__':
	#test modules behavior
	t = ClusterTask('test task', 'command -i {input}', modules=('tm1', 'tm2', 'tm3'))
	t.input_files = ['path/to/input.file']
	t.is_root()
	c = ClusterTask('test task 2', 'command', modules=(('tm4',)))
	c.follows(t)
	print t.start()
	print t.scan_modules()
		
