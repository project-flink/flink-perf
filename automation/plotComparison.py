import numpy as np
import matplotlib.pyplot as plt
import sys


filename = sys.argv[1]
data = np.loadtxt(filename,delimiter=',',skiprows=1,usecols=(1,2,3,4,5,6,7)).T

fig, ax = plt.subplots(2,3)

ax[0][0].set_title("Word Count")
ax[0][0].set_xlim(-1, 3.5)
ax[0][0].set_xticks([0.5, 2.5])
ax[0][0].set_xticklabels(["spark", "flink"])
with_combine = ax[0][0].bar(left=(0,2), height=(data[0][0], data[0][1]),  width=0.5, color="orange", yerr=0.01)
without_combine = ax[0][0].bar(left=(0.5,2.5), height=(data[1][0], data[1][1]), width=0.5, color="blue", yerr=0.01)
ax[0][0].legend([with_combine, without_combine], ["w/ combine", "w/o combine"],loc='best',fancybox=True,fontsize=10)

ax[0][1].set_title("Connected Components")
ax[0][1].set_xlim(-1, 2)
ax[0][1].set_xticks([0, 1])
ax[0][1].set_xticklabels(["spark", "flink"])
ax[0][1].bar(left=(0,1), height=(data[2][0],data[2][1]), align="center", width=0.5, color=["orange","blue"],yerr=0.01)

ax[0][2].set_title("TPCH 3")
ax[0][2].set_xlim(-1, 2)
ax[0][2].set_xticks([0, 1])
ax[0][2].set_xticklabels(["spark", "flink"])
ax[0][2].bar(left=(0,1), height=(data[5][0],data[5][1]), align="center", width=0.5, color=["orange","blue"],yerr=0.01)

ax[1][0].set_title("KMeans(D=3, K=8)")
ax[1][0].set_xlim(-1, 2)
ax[1][0].set_xticks([0, 1])
ax[1][0].set_xticklabels(["spark", "flink"])
ax[1][0].bar(left=(0,1), height=(data[3][0],data[3][1]), align="center", width=0.5, color=["orange","blue"],yerr=0.01)

ax[1][1].set_title("KMeans(D=1000, K=800)")
ax[1][1].set_xlim(-1, 2)
ax[1][1].set_xticks([0, 1])
ax[1][1].set_xticklabels(["spark", "flink"])
ax[1][1].bar(left=(0,1), height=(data[4][0],data[4][1]), align="center", width=0.5, color=["orange","blue"],yerr=0.01)

ax[1][2].set_title("Page Rank")
ax[1][2].set_xlim(-1, 2)
ax[1][2].set_xticks([0, 1])
ax[1][2].set_xticklabels(["spark", "flink"])
ax[1][2].bar(left=(0,1), height=(data[6][0],data[6][1]), align="center", width=0.5, color=["orange","blue"],yerr=0.01)

ax[0][0].set_ylabel("Runtime(s)")
ax[1][0].set_ylabel("Runtime(s)")
for i in range(2):
	for j in range(3):
		ax[i][j].grid(True)
plt.show()

