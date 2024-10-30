import os, sys

index_table = sys.argv[1]

# read the question from stdin
data = sys.stdin.read()

# get the embedding of the question from LLM

# run SQL with index table and get N-Best results

# submit the N-best result, the question to LLM and dialogue history to get the final answer

# send the answer to stdout
print('["this is answer from LLM with index table name = %s. Question: %s"]' % (sys.argv[1], str(data)))
