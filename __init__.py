
import json
import re

STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_FINISH = 'completed'
STATUS_FATAL = 'fatal error'


class FatalException(Exception):
	pass
	
	
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


class Task():
	
	def __init__(self,
				 name,
				 run,
				 **kwargs):
		self.name = name
		self.slug = _slugify(name)
		self.run_command = run
		self.params = kwargs
		self.__children__ = set()
		self.status = STATUS_PENDING
		self.job_id = None
	
	def depends_on(self, parent):
		if not isinstance(parent, Task):
			raise ValueError("Graph relationships must be between Task objects.")
		parent.__children__.add(self)
		
	
	def build(self, **kwargs):
		params = dict()
		params.update(self.params)
		params.update(kwargs)
		return self.run_command.format(**params)
		
	def __str__(self):
		if self.__children__:
			return "Task {0}, followed by: {1}".format(self.name, ', '.join([c.name for c in self.__children__]))
		else:
			return "Task {0}".format(self.name)
	def __repr__(self):
		return self.slug
		
	def to_dict(self):
		s = dict()
		s.update(self.__dict__)
		del s['__children__']
		s['~children'] = [c.to_dict() for c in self.__children__]
		return s
		
	def from_dict(self, s):
		for key, value in s.items():
			if '~' not in key:
				setattr(self, key, value)
		for child in self.__children__:
			for struct in s['~children']:
				if struct['name'] == child.name:
					child.from_dict(struct)
		
		
	def serialize(self):
		s = self.to_dict()
		return json.dumps(s, ensure_ascii=True, indent=2, separators=(',', ': '), sort_keys=True)
		
		
	def deserialize(self, ser):
		struct = json.loads(ser)
		self.from_dict(struct)
		
	def get_next(self, nexts=set()):
		if self.status == STATUS_FATAL:
			raise FatalException("Task {s.name} (job id:{s.job_id}) is in a terminal failure state.".format(s=self))
		if self.status == STATUS_PENDING or self.status == STATUS_RUNNING:
			nexts.add(self)
		if self.status == STATUS_FINISH:
			[c.get_next(nexts) for c in self.__children__]
		return nexts
			
		
if __name__ == '__main__':
	t1 = Task('Task 1',
			  """echo "test 1" """,
			  flavor="lemon")
	t2 = Task('This -- is a ## test ---',
			  "ls -lah") #test slugify
	t3 = Task("test_3",
			  "spades.py -1 {forward} -2 {reverse} -o {output} -f {flavor}",
			  flavor="banana")
	t2.depends_on(t1)
	t3.depends_on(t1)
	t1.job_id = 12345
	s = t1.serialize()
	print s
	t1.deserialize(s)
	print t1
	print t1.serialize() == s
	t1.status = STATUS_FINISH
	print t1.get_next()
	t2.status = STATUS_FATAL
	try:
		print t1.get_next()
		raise Exception("Didn't throw the right exception")
	except FatalException as e:
		print e
	print t3.build(forward="test_forward", reverse="test_reverse", output="test_output")
	