CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra

# Arrow / Parquet — always available via pkg-config on this system
ARROW_CFLAGS := $(shell pkg-config --cflags arrow parquet 2>/dev/null)
ARROW_LIBS   := $(shell pkg-config --libs   arrow parquet 2>/dev/null || echo "-lparquet -larrow")

# mosquitto — dev headers installed via: sudo apt install libmosquitto-dev
MOSQ_CFLAGS  := $(shell pkg-config --cflags libmosquitto 2>/dev/null)
MOSQ_LIBS    := $(shell pkg-config --libs   libmosquitto 2>/dev/null || echo "-lmosquitto")

# yaml-cpp — dev headers installed via: sudo apt install libyaml-cpp-dev
YAML_CFLAGS  := $(shell pkg-config --cflags yaml-cpp 2>/dev/null)
YAML_LIBS    := $(shell pkg-config --libs   yaml-cpp 2>/dev/null || echo "-lyaml-cpp")

# simdjson — installed via: sudo apt install libsimdjson-dev
SIMDJSON_CFLAGS := $(shell pkg-config --cflags simdjson 2>/dev/null)
SIMDJSON_LIBS   := $(shell pkg-config --libs   simdjson 2>/dev/null || echo "-lsimdjson")

ALL_CFLAGS := $(CXXFLAGS) $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(YAML_CFLAGS) $(SIMDJSON_CFLAGS)
ALL_LIBS   := $(ARROW_LIBS) $(MOSQ_LIBS) $(YAML_LIBS) $(SIMDJSON_LIBS)

# Python with pyarrow — used by test helpers to read back written Parquet files.
# Try venv locations common to both phil-dev and spark-22b6 / .51.
TEST_PYTHON  := $(shell \
  ls $(HOME)/data_server/.venv/bin/python3 2>/dev/null || \
  ls $(HOME)/work/gen-ai/data_server/.venv/bin/python3 2>/dev/null || \
  echo python3)

TEST_FLAGS := $(CXXFLAGS) -DPARQUET_WRITER_NO_MAIN \
              -DTEST_PYTHON_BIN=\"$(TEST_PYTHON)\" \
              $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(YAML_CFLAGS) $(JSON_CFLAGS)

.PHONY: all clean deps test kpi

all: parquet_writer

parquet_writer: writer.cpp
	$(CXX) $(ALL_CFLAGS) $< -o $@ $(ALL_LIBS)

tests/test_writer: tests/test_writer.cpp writer.cpp tests/test_runner.h
	$(CXX) $(TEST_FLAGS) -I. tests/test_writer.cpp -o $@ $(ALL_LIBS)

tests/kpi_writer: tests/kpi_writer.cpp writer.cpp
	$(CXX) $(TEST_FLAGS) -I. tests/kpi_writer.cpp -o $@ $(ALL_LIBS)

test: tests/test_writer
	./tests/test_writer

kpi: tests/kpi_writer
	./tests/kpi_writer

# Install missing dev packages (requires sudo)
deps:
	sudo apt-get install -y libmosquitto-dev libyaml-cpp-dev libsimdjson-dev

clean:
	rm -f parquet_writer tests/test_writer tests/kpi_writer
