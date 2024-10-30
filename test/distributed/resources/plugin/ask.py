import os, sys

data = sys.stdin.read()
print('["this is answer from LLM with index table name = %s. Question: %s"]' % (sys.argv[1], str(data)))
