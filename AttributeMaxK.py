#!/usr/bin/python
 
import sys
 
index = int(sys.argv[1])
maxK = int(sys.argv[2])
mx = []
 
for line in sys.stdin:
 values = line.strip().split(",")
 if values[index].isdigit():
  mx.append(int(values[index]))
else:
 maxValues = set(mx)
 mx = list(maxValues)
 mx.sort()
 mx = mx[-maxK:]
 for value in mx:
  print value


