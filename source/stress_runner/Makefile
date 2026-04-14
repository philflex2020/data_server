CXX      := g++
CXXFLAGS := -O2 -std=c++17 -Wall

SIMDJSON_LIBS := $(shell pkg-config --libs simdjson 2>/dev/null || echo "-lsimdjson")
SIMDJSON_INC  := $(shell pkg-config --cflags simdjson 2>/dev/null)

.PHONY: all clean

all: stress_pub stress_real_pub ems_site_simulator

stress_pub: stress_pub.cpp
	$(CXX) $(CXXFLAGS) $< -o $@ -lmosquitto

stress_real_pub: stress_real_pub.cpp
	$(CXX) $(CXXFLAGS) $(SIMDJSON_INC) $< -o $@ -lmosquitto $(SIMDJSON_LIBS)

# ems_site_simulator — successor to stress_real_pub, same source
ems_site_simulator: ems_site_simulator.cpp
	$(CXX) $(CXXFLAGS) $(SIMDJSON_INC) $< -o $@ -lmosquitto $(SIMDJSON_LIBS)

clean:
	rm -f stress_pub stress_real_pub ems_site_simulator
