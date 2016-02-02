import unittest
from unittest import TestCase as Case
from regex import RegexTask, STATUS_FATAL

class Reg(Case):
	
	def setUp(self):
		self.R = RegexTask("Test Regex Task",
						   "(?P<PARAM>^TEST\w*)",
						   stop_on_miss=True)
	
	
class TestMatchCase(Reg):
	
	def testMatch(self):
		self.R.stdout("TEST_PARAMETER")
		self.assertIn(("PARAM","TEST_PARAMETER"), self.R.termargs)
		
class TestNonMatchCase(Reg):

	def testNonMatch(self):
		self.R.stdout("PARAMETER")
		self.assertNotIn(("PARAM","PARAMETER"), self.R.termargs)
		self.assertEqual(self.R.status, STATUS_FATAL)