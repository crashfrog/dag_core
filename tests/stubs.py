class Orm(object):
	
	def __getattr__(self, atr):
		try:
			return super(Orm, self).__getattr__(atr)
		except AttributeError:
			return atr
			
	def getNamedPaths(self):
		return {}
	
class Meta(object):

	FASTQ_FILE = '/path/to/a/file.fastq'
	FASTA_FILE = '/path/to/another/file.fasta'
	SEQUIN_FILE = '/path/to/yet/another/file.sqn'
	TEXT_FILE = '/path/to/a/text/file.txt'
	

	
	
class RecordNotFoundException(Exception):
	pass