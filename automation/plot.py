import numpy as np
import matplotlib.pyplot as plt
import sys


filename = sys.argv[1]
data = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(1,2,3)).T
label = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(0,),dtype=str)

plt.plot(data[0], "ro-", label="word count")
plt.plot(data[1], "go-", label="connected components")
plt.plot(data[2], "bo-", label="testjob")
top=0.0
for e in data:
	try:
		if (max(e) > top):
			top=max(e)
	except:
		if (e > top):
			top=e

plt.ylim(0,1.1*top)

try:
	plt.xlim(-0.35, len(label))
	plt.xticks(np.arange(len(label)), label)
except:
	plt.xlim(-0.35, 1)
	plt.xticks([0], [label])

locs, labels = plt.xticks()
plt.setp(labels, rotation=45)
plt.legend(loc='best',fancybox=True)
plt.grid(True)
plt.title("Performance Evaluation")
plt.ylabel("Runtime(sec)")
plt.tight_layout()
plt.show()

