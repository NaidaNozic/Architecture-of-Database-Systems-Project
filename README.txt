############################
## ADBS WS2021/22 Project ##
############################

NOTE: Developed and compiled in gcc version 9.3.0
        
Quick Start Guide
-----------------

# Compile and run unit tests
make unit_test
./unit_test

# Compile and run speed tests
make speed_test
./speed_test

# Compile all
make all

# Clean
make clean
        
Hardware properties
-------------------

# Retrieve decisive processor properties and insert them into hardware.h
./init_hardware.sh
        
# Only works on a GNU/Linux system, if you use another operating system, please
# insert the numbers manually (optional, just if you want to use them in your
# implementation of the groupByAggOperator), or just leave them at zero (in
# which case you won't be able to use them).

Scoring
-------

# Compile, record runtime of naive baseline (if not done before), and score
# your implementation
make scoring

# More precisely, for automatically calculating the score, we need the runtime
# of the naive baseline and your implementation. As the former remains
# unchanged, it can be recorded once by:
make speed_test_baseline
./speed_test_baseline 0 rec times_baseline.csv
# Once recorded, you can have your score calculated by
./speed_test 0 ref times_baseline.csv

Debugging
---------

# Run a specific unit test case (e.g. 2.34) and show input/output data
./unit_test 1 2 34

# Run a specific speed test case
./speed_test 1

# Enable debug printing for speed tests
set debug = 1 in the code (speedtest.c)

# Compile with debugging flags
make debug_unit
make debug_speed

Change Log
----------

v1: initial test suite, benchmark, make file, reference implementation
v2: bug fixes, improvements to make file
v3: automatic scoring and simple hardware abstraction

Migration notes v2 to v3
------------------------

If you have already started your implementation of the groupByAgg-operator, you
can simply replace the file groupByAgg.cpp by your existing file. If you have
added more files, simply copy them to the new directory. If you have changed
other files (which you don't need to do), make sure to transfer these changes
as well.