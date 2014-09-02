import numpy as np
import sys
import random

def get_next(x):
	i = 0
	new_x = np.copy(x)
	while new_x[i] == 1:
		i = i + 1
	new_x[i] = 1
	for j in range(i):
		new_x[j] = 0
	return new_x

D = int(sys.argv[1])
K = int(sys.argv[2])
num = int(sys.argv[3])

point_file = open(sys.argv[4], "w")
center_file = open(sys.argv[5], "w")

c = np.zeros((K, D))
for i in range(1, K):
	c[i] = get_next(c[i-1])


point = np.zeros(D)
count = 0
for k in range(num):
	i = np.random.randint(K)
	count = count + 1
	print >> point_file, count,
	for j in range(D):
		point[j] = c[i][j] * 100.0 + np.random.random() * 40 - 20
		print >> point_file, point[j], 
	print >> point_file

for i in range(K):
	print >> center_file, i + 1,
	for j in range(D):
		point[j] = c[i][j] * 100.0 + np.random.random() * 60 - 30
		print >> center_file, point[j],
	print >> center_file


