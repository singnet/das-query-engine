#!/bin/bash

# change 0.06 to define threshold of color considered white (0.01 if basically #ffffff)
# change 0.0 to set tghe level of transparency (0.0 is 100% transparent)

ffmpeg -i $1 -vf colorkey=white:0.06:0.0 $2
