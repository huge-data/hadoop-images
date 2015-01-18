#! /usr/bin/env python
import numpy as np
import cv2
import sys

#find frames per second of video
cap = cv2.VideoCapture(str(sys.argv[1]))
fps = cap.get(5)
print fps
cap.release()
