CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra

# Arrow / Parquet
ARROW_CFLAGS := $(shell pkg-config --cflags arrow parquet 2>/dev/null)
ARROW_LIBS   := $(shell pkg-config --libs   arrow parquet 2>/dev/null || echo "-lparquet -larrow")

# mosquitto
MOSQ_CFLAGS  := $(shell pkg-config --cflags libmosquitto 2>/dev/null)
MOSQ_LIBS    := $(shell pkg-config --libs   libmosquitto 2>/dev/null || echo "-lmosquitto")

# simdjson
SIMDJSON_CFLAGS := $(shell pkg-config --cflags simdjson 2>/dev/null)
SIMDJSON_LIBS   := $(shell pkg-config --libs   simdjson 2>/dev/null || echo "-lsimdjson")

ALL_CFLAGS := $(CXXFLAGS) $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(SIMDJSON_CFLAGS)
ALL_LIBS   := $(ARROW_LIBS) $(MOSQ_LIBS) $(SIMDJSON_LIBS)

.PHONY: all clean

all: parquet_sync_checker

parquet_sync_checker: checker.cpp
	$(CXX) $(ALL_CFLAGS) $< -o $@ $(ALL_LIBS)

clean:
	rm -f parquet_sync_checker
