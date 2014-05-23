import numpy as np
import matplotlib.pyplot as plt
import sys


filename = sys.argv[1]
data = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(1,2)).T
label = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(0,),dtype=str)

plt.plot(data[0], "ro-", label="wordcount")
plt.plot(data[1], "bo-", label="testjob")
plt.ylim(0,1.1*max(data[1]))
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

