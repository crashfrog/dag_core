import unittest
from unittest import TestCase as Case
import dag_core
from dag_core import *

TEST_PARAMETER = 'TEST_PARAMETER'
TEST_VALUE = 'TEST_VALUE'
TEST_NOT_VALUE = 'TEST_NOT_VALUE'


class Task(Case):

	def setUp(self):
		self.A = A = AbstractTask("Root Task A", TEST_PARAMETER=TEST_VALUE)
		self.B = B = AbstractTask("Task B")
		
		B.follows(A)
		
class TestParameterBindingCase(Task):
	
	def testParameterBinding(self):
		self.A.__start__()
		self.A.__finalize__({}, '', [], 'VAL1', 'VAL2', TEST_PARAMETER_2=TEST_NOT_VALUE)
		self.assertIn('VAL1', self.B.termargs)
		self.assertIn('VAL2', self.B.termargs)
		self.assertIn((TEST_PARAMETER, TEST_VALUE), self.B.termargs)
		self.assertIn(('TEST_PARAMETER_2', TEST_NOT_VALUE), self.B.termargs)
		
class TestParameterBindingOverrideCase(Task):

	def testParameterOverride(self):
		self.A.__start__()
		self.A.__finalize__({}, '', [], TEST_PARAMETER=TEST_NOT_VALUE)
		self.assertIn((TEST_PARAMETER, TEST_NOT_VALUE), self.B.termargs)


class DAG(Case):
	
	def setUp(self):
		self.A = A = AbstractTask("Root Task A")
		self.B = B = AbstractTask("Task B", file_filter="*fasta")
		self.C = C = AbstractTask("Task C")
		self.D = D = AbstractTask("Task D")
		self.E = E = AbstractTask("Task E")
		
		A.is_root()
		
		B.follows(A)
		C.follows(A)
		D.follows(C)
		E.follows(C).when(TEST_PARAMETER, TEST_VALUE)
		
		
	def tearDown(self):
		pass
		
class TestSlugifyCase(DAG):

	def testSlugify(self):
		self.assertEqual(self.A.__getSlug__(), "root-task-a")

class TestRootCase(DAG):

	def testRoot(self):
		self.assertIs(self.A, tasks._root)

class TestStructureCase(DAG):

	def testStructure(self):
		self.assertIn(self.B, self.A.__children__)
		self.assertIn(self.D, self.C.__children__)
		
class TestSerializeCase(DAG):
	
	def testSerialize(self):
		s = self.A.__serialize__()
		self.A.__deserialize__(s)
		self.assertEqual(self.A.__serialize__(), s)
		
class TestGetNextCase(DAG):
	
	def testGetNext(self):
		self.A.status = STATUS_FINISH
		self.assertEqual(self.A.__get_next__(), set([self.B, self.C]))
		
class TestFinalizeCase(DAG):
	
	def testFinalize(self):
		self.A.__finalize__({}, '', ('/path/to/a/file.fastq', '/path/to/another/file.fasta'))
		self.assertEqual(self.A.status, STATUS_FINISH)
		self.assertIn('/path/to/another/file.fasta', self.B.input_files)
		self.assertNotIn('/path/to/a/file.fastq', self.B.input_files)
		
class Cond(DAG):

	def setUp(self):
		super(Cond, self).setUp()
		self.A.status = STATUS_FINISH
		self.B.status = STATUS_FINISH
		
class TestNegativeConditionalCase(Cond):

	def testNegativeConditional(self):
		self.C.__finalize__({}, '', [], TEST_PARAMETER = TEST_NOT_VALUE)
		self.assertIn(self.D, self.A.__get_next__())
		self.assertNotIn(self.E, self.A.__get_next__())
		
class TestPositiveConditionalCase(Cond):

	def testPositiveConditional(self):
		self.C.__finalize__({}, '', [], TEST_PARAMETER = TEST_VALUE)
		self.assertIn(self.D, self.A.__get_next__())
		self.assertIn(self.E, self.A.__get_next__())
	
class Dec(DAG):
	
	def setUp(self):
		super(Dec, self).setUp()
		
		self.prep_ran = False
		self.post_ran = False
		
		@self.A.prep
		def prepTest(*a, **kw):
			self.prep_ran = True
			
		@self.A.post
		def postTest(*a, **kw):
			self.post_ran = True
		
class TestPreprocessorDecoratorCase(Dec):
	
	def testPrepDecorator(self):
		self.A.__start__()
		self.assertTrue(self.prep_ran)
		
class TestPostprocessorDecoratorCase(Dec):
	
	def testPostDecorator(self):
		self.A.__finalize__({}, '', [])
		self.assertTrue(self.post_ran)
	



if __name__ == "__main__":
	unittest.main()