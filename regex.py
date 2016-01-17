from tasks import AbstractTask, STATUS_FINISH, STATUS_FATAL
import re

class RegexTask(AbstractTask):
	def __init__(self, name, regex, stop_on_miss=False, **kwargs):
		super(RegexTask, self).__init__(name, "", **kwargs)
		# del self.run_command
		self.regex = re.compile(regex)
		self.stop_miss = stop_on_miss
		
# 	def start(self, **kwargs):
# 		self.status = STATUS_FINISH
# 		return None
		
	def stdout(self, stdoutput):
		mtch = self.regex.search(stdoutput)
		if mtch:
			for key, value in mtch.groupdict().items():
				if 'default' not in key:
					self.termargs.append((key, value))
		else:
			if self.stop_miss:
				self.status = STATUS_FATAL