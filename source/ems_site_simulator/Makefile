CXX      := g++
CXXFLAGS := -O2 -std=c++20 -Wall -Wextra

SIMDJSON_LIBS := $(shell pkg-config --libs simdjson 2>/dev/null || echo "-lsimdjson")
SIMDJSON_INC  := $(shell pkg-config --cflags simdjson 2>/dev/null)

.PHONY: all clean

all: ems_site_simulator

ems_site_simulator: ems_site_simulator.cpp
	$(CXX) $(CXXFLAGS) $(SIMDJSON_INC) $< -o $@ -lmosquitto $(SIMDJSON_LIBS)

clean:
	rm -f ems_site_simulator
