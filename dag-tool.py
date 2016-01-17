import sys
import os, os.path

usage = """
DAG workflow verification tool v 0.01
"""


try:
	dag_file = os.path.abspath(sys.argv[1])
except IndexError:
	print usage
	quit()
	
with open(dag_file, 'rU') as dag_source:
	for line in dag_source:
		if line.startswith('#'):
			print line.lstrip('#').rstrip('\n').replace('_', '\_')
		else:
			print '    ' + line.replace('\t', '    ').rstrip('\n')