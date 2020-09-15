import os

name = os.getenv("TEST_LOC", "temp")
f = open(name + "/log").read().split('\n')
f = f[:-1]

result = {}

for i in f:
    if i[:4] == "INFO":
        a,b= i.split(" ")[1:]
        a,b = int(a), int(b)

        result[a]=b


x = []
y = []

for i in sorted(result.keys()):
    x.append(i)
    y.append(result[i])

import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

plt.plot(x, y)

plt.savefig(name + "/image_log.png")
