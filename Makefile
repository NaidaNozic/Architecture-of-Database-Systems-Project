# *****************************************************************************
# Config
# *****************************************************************************

# Compiler.
CC = gcc
CCPP = g++

# Compiler flags.
GENERAL = -Wall
OPT = -O3
DBG = -g

# Source and object file names.
SRCS_C = miscsvc.c utils.c
SRCS_C_UT = $(SRCS_C) unittest.c
SRCS_C_ST = $(SRCS_C) speedtest.c
SRCS_CPP = groupByAgg.cpp match.cpp
SRCS_CPP_BASE = groupByAggBaseline.cpp match.cpp
OBJS = miscsvc.o match.o utils.o
OBJS_UT = $(OBJS) unittest.o groupByAgg.o
OBJS_ST = $(OBJS) speedtest.o groupByAgg.o
OBJS_ST_BASE = $(OBJS) speedtest.o groupByAggBaseline.o 

# Targets names.
EXE_UT = unit_test
EXE_ST = speed_test
DBG_UT = debug_unit
DBG_ST = debug_speed
EXE_ST_BASE = speed_test_baseline
BASELINE = times_baseline.csv

# *****************************************************************************
# Targets
# *****************************************************************************

all: $(EXE_UT) $(EXE_ST)

$(EXE_UT): $(SRCS_C_UT) $(SRCS_CPP) *.h
	$(CC)   -c $(GENERAL) $(OPT) $(SRCS_C_UT)
	$(CCPP) -c $(GENERAL) $(OPT) $(SRCS_CPP)
	$(CCPP) -o $(EXE_UT) $(OBJS_UT)
	rm -f $(OBJS_UT)

$(DBG_UT): $(SRCS_C_UT) $(SRCS_CPP) *.h
	$(CC)   -c $(GENERAL) $(DBG) $(SRCS_C_UT)
	$(CCPP) -c $(GENERAL) $(DBG) $(SRCS_CPP)
	$(CCPP) -o $(DBG_UT) $(OBJS_UT)
	rm -f $(OBJS_UT)

$(EXE_ST): $(SRCS_C_ST) $(SRCS_CPP) *.h
	$(CC)   -c $(GENERAL) $(OPT) $(SRCS_C_ST)
	$(CCPP) -c $(GENERAL) $(OPT) $(SRCS_CPP)
	$(CCPP) -o $(EXE_ST) $(OBJS_ST)
	rm -f $(OBJS_ST)

$(EXE_ST_BASE): $(SRCS_C_ST) $(SRCS_CPP_BASE) *.h
	$(CC)   -c $(GENERAL) $(OPT) $(SRCS_C_ST)
	$(CCPP) -c $(GENERAL) $(OPT) $(SRCS_CPP_BASE)
	$(CCPP) -o $(EXE_ST_BASE) $(OBJS_ST_BASE)
	rm -f $(OBJS_ST_BASE)

$(DBG_ST): $(SRCS_C_ST) $(SRCS_CPP) *.h
	$(CC)   -c $(GENERAL) $(DBG) $(SRCS_C_ST)
	$(CCPP) -c $(GENERAL) $(DBG) $(SRCS_CPP)
	$(CCPP) -o $(DBG_ST) $(OBJS_ST)
	rm -f $(OBJS_ST)

test: all
  ifeq ($(OS),Windows_NT)
		./$(EXE_UT).exe
		./$(EXE_ST).exe
  else #Linux
		./$(EXE_UT)
		./$(EXE_ST)
  endif

$(BASELINE): $(EXE_ST_BASE)
	./$(EXE_ST_BASE) 0 rec $(BASELINE)
	
scoring: $(EXE_ST) $(BASELINE)
	./$(EXE_ST) 0 ref $(BASELINE)

clean:
	rm -f $(EXE_UT) $(EXE_ST) $(DBG_UT) $(DBG_ST) $(OBJS_UT) $(OBJS_ST) $(BASELINE)
