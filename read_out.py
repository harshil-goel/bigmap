import os

name = os.getenv("TEST_LOC", "temp")
f = open(name + "/out").read().split('\n')
f = f[:-1]

result = {}

while len(f) > 0:
    a,b = f[:2]
    f = f[2:]

    aa = int(a.split("_")[1])
    c = b.split()[-2][-2:]
    bb = float(b.split()[-2][:-2])
    if c != "MB":
        bb *= 1024
    result[aa] = bb


x = []
y = []

for i in sorted(result.keys()):
    x.append(i)
    y.append(result[i])

import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
plt.plot(x, y)

plt.savefig(name + "/image_out.png")

