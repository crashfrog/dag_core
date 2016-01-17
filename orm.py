from tasks import AbstractTask

try:
	from gov.fda.cfsan.slims.util.orm import Orm, RecordNotFoundException
	from gov.fda.cfsan.slims.util import Meta
except ImportError:
	from tests.stubs import Orm, RecordNotFoundException, Meta


class OrmShadow(object):
	"dumb object to prevent accidental database changes"
	def __init__(self, orm_to_shadow):
		for key, value in orm_to_shadow.dict().items():
			if '__' not in key:
				setattr(self, key, value)
		if hasattr(orm_to_shadow, "getNamedPaths"):
			for field, path in orm_to_shadow.getNamedPaths():
				setattr(self, repr(field), path)


class OrmTask(AbstractTask):
	
	def __init__(self, types, **kwargs):
		super(OrmTask, self).__init__("ORM Task", **kwargs)
		if isinstance(types, str):
			self.types = (types,)
		elif hasattr(types, '__iter__'):
			self.types = types
		else:
			raise ValueError("Types parameter should be string or list of strings.")
		
	
class OrmCreatorTask(AbstractTask):

	def __init__(self, type, **kwargs):
		super(NewOrmTask, self).__init__("ORM Creator Task")


class RemapTask(AbstractTask):
	
	def __init__(self, name, exclude=False, **kwargs):
		super(RemapTask, self).__init__(name)
		self.remap = {}.update(kwargs)
		