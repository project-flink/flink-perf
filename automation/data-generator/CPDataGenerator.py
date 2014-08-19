import numpy as np
import sys
import sets
import random

vertexNum = int(sys.argv[1])
edgeNum = int(sys.argv[2])

vertex = sys.argv[3]
edge = sys.argv[4]

vertexfile=open(vertex, "w")
edgefile=open(edge, "w")
for i in range(vertexNum):
	print >> vertexfile, i

e = sets.Set()
while len(e) < edgeNum:
	v1 = np.random.randint(vertexNum - 1)
	v2 = np.random.randint(v1 + 1, vertexNum)
	e.add((v1, v2))

for ei in e:
	print >> edgefile,ei[0],ei[1]
