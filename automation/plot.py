import numpy as np
import matplotlib.pyplot as plt
import sys


filename = sys.argv[1]
data = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(1,2,3,4,5,6,7)).T
label = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(0,),dtype=str)

fig, ax = plt.subplots(3,2, sharex=True)

ax[0][0].plot(data[0], "ro-", label="w/ combine")
ax[0][0].plot(data[1], "bo-", label="w/o combine")
ax[0][1].plot(data[2], "ro-", label="connected components")
ax[1][0].plot(data[3], "ro-", label="kmeans(D=3, K=8)")
ax[1][1].plot(data[4], "ro-", label="kmeans(D=1000, K=800)")
ax[2][0].plot(data[5], "ro-", label="TPCH3")
ax[2][1].plot(data[6], "ro-", label="Page Rank")
try:
	y1 = min(min(data[0]), min(data[1]))
	y2 = max(max(data[0]), max(data[1]))
	ax[0][0].set_ylim([0.9*y1,1.1*y2])
	ax[0][1].set_ylim([0.9*min(data[2]), 1.1*max(data[2])])
	ax[1][0].set_ylim([0.9*min(data[3]), 1.1*max(data[3])])
	ax[1][1].set_ylim([0.9*min(data[4]), 1.1*max(data[4])])
	ax[2][0].set_ylim([0.9*min(data[5]), 1.1*max(data[5])])
	ax[2][1].set_ylim([0.9*min(data[6]), 1.1*max(data[6])])
except:
	ax[0][0].set_ylim([0.9*min(data[0],data[1]), 1.1*max(data[0],data[1])])
	ax[0][1].set_ylim([0.9*data[2], 1.1*data[2]])
	ax[1][0].set_ylim([0.9*data[3], 1.1*data[3]])
	ax[1][1].set_ylim([0.9*data[4], 1.1*data[4]])
	ax[2][0].set_ylim([0.9*data[5], 1.1*data[5]])
	ax[2][1].set_ylim([0.9*data[6], 1.1*data[6]])


try:
	ax[2][0].set_xlim(-0.35, len(label))
	ax[2][0].set_xticks(np.arange(len(label)))
	ax[2][0].set_xticklabels(label, rotation=30)

	ax[2][1].set_xlim(-0.35, len(label))
	ax[2][1].set_xticks(np.arange(len(label)))
	ax[2][1].set_xticklabels(label, rotation=30)

except:
	ax[2][0].set_xlim(-0.35, 1)
	ax[2][0].set_xticks([0])
	ax[2][0].set_xticklabels([label], rotation=30)

	ax[2][1].set_xlim(-0.35, 1)
	ax[2][1].set_xticks([0])
	ax[2][1].set_xticklabels([label], rotation=30)


ax[0][0].legend(loc='best',fancybox=True,fontsize=10)
ax[0][0].set_title("Word Count")
ax[0][1].set_title("Connected Components")
ax[1][0].set_title("KMeans(D=3, K=8)")
ax[1][1].set_title("KMeans(D=1000, K=800)")
ax[2][0].set_title("TPCH3")
ax[2][1].set_title("Page Rank")
ax[0][0].set_ylabel("Runtime(s)")
ax[1][0].set_ylabel("Runtime(s)")
ax[2][0].set_ylabel("Runtime(s)")

plt.show()

