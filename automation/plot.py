import numpy as np
import matplotlib.pyplot as plt
import sys


filename = sys.argv[1]
data = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(1,2,3,4,5)).T
label = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(0,),dtype=str)

fig, ax = plt.subplots(2,2, sharex=True)

ax[0][0].plot(data[0], "ro-", label="word count")
ax[0][0].plot(data[1], "bo-", label="word count without combine")
ax[0][1].plot(data[2], "ro-", label="connected components")
ax[1][0].plot(data[3], "ro-", label="kmeans")
ax[1][1].plot(data[4], "ro-", label="testjob")
try:
	y1 = min(min(data[0]), min(data[1]))
	y2 = max(max(data[0]), max(data[1]))
	ax[0][0].set_ylim([0.9*y1,1.1*y2])
	ax[0][1].set_ylim([0.9*min(data[2]), 1.1*max(data[2])])
	ax[1][0].set_ylim([0.9*min(data[3]), 1.1*max(data[3])])
	ax[1][1].set_ylim([0.9*min(data[4]), 1.1*max(data[4])])
except:
	ax[0][0].set_ylim([0.9*min(data[0],data[1]), 1.1*max(data[0],data[1])])
	ax[0][1].set_ylim([0.9*data[2], 1.1*data[2]])
	ax[1][0].set_ylim([0.9*data[3], 1.1*data[3]])
	ax[1][1].set_ylim([0.9*data[4], 1.1*data[4]])

try:
	ax[1][0].set_xlim(-0.35, len(label))
	ax[1][0].set_xticks(np.arange(len(label)))
	ax[1][0].set_xticklabels(label, rotation=30)

	ax[1][1].set_xlim(-0.35, len(label))
	ax[1][1].set_xticks(np.arange(len(label)))
	ax[1][1].set_xticklabels(label, rotation=30)
except:
	ax[1][0].set_xlim(-0.35, 1)
	ax[1][0].set_xticks([0])
	ax[1][0].set_xticklabels([label], rotation=30)

	ax[1][1].set_xlim(-0.35, 1)
	ax[1][1].set_xticks([0])
	ax[1][1].set_xticklabels([label], rotation=30)


ax[0][0].legend(loc='best',fancybox=True,fontsize=10)
ax[0][0].set_title("Word Count")
ax[0][1].set_title("Connected Components")
ax[1][0].set_title("KMeans")
ax[1][1].set_title("Testjob")
ax[0][0].set_ylabel("Runtime(s)")
ax[1][0].set_ylabel("Runtime(s)")

plt.tight_layout()
plt.show()

