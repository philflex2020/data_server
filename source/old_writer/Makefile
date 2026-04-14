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
              $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(YAML_CFLAGS) $(SIMDJSON_CFLAGS)

REAL_TEST_FLAGS := -std=c++20 -O2 -Wall -Wextra -DREAL_WRITER_NO_MAIN \
              -DTEST_PYTHON_BIN=\"$(TEST_PYTHON)\" \
              $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(YAML_CFLAGS) $(SIMDJSON_CFLAGS)

# --------------------------------------------------------------------------
# Dependency checker — run before any build target
# --------------------------------------------------------------------------
# Each check emits a suggestion and sets a flag file on failure so Make can
# abort with a single consolidated error message.
define CHECK_PKG  # (pkg, apt-pkg, extra-hint)
	@if ! pkg-config --exists $(1) 2>/dev/null; then \
	  echo ""; \
	  echo "  MISSING: $(1)"; \
	  echo "    apt:  sudo apt-get install -y $(2)"; \
	  if [ -n "$(3)" ]; then echo "    note: $(3)"; fi; \
	  touch /tmp/.missing_deps; \
	fi
endef

.PHONY: check-deps
check-deps:
	@rm -f /tmp/.missing_deps
	@echo "Checking build dependencies..."
	$(call CHECK_PKG,arrow,\
	  libarrow-dev libparquet-dev,\
	  if not in apt: https://arrow.apache.org/install/ or build from source)
	$(call CHECK_PKG,parquet,\
	  libparquet-dev,\
	  usually installed alongside libarrow-dev)
	$(call CHECK_PKG,libmosquitto,\
	  libmosquitto-dev,\
	  broker also needs: sudo apt-get install -y mosquitto)
	$(call CHECK_PKG,yaml-cpp,\
	  libyaml-cpp-dev)
	@if ! pkg-config --exists simdjson 2>/dev/null && \
	    ! echo | $(CXX) -x c++ - -include simdjson.h -fsyntax-only 2>/dev/null; then \
	  echo ""; \
	  echo "  MISSING: simdjson"; \
	  echo "    apt:  sudo apt-get install -y libsimdjson-dev"; \
	  echo "    note: Ubuntu <22.04: build from source — https://github.com/simdjson/simdjson"; \
	  touch /tmp/.missing_deps; \
	fi
	@if ! command -v $(CXX) >/dev/null 2>&1; then \
	  echo ""; \
	  echo "  MISSING: $(CXX) compiler"; \
	  echo "    apt:  sudo apt-get install -y g++ build-essential"; \
	  touch /tmp/.missing_deps; \
	else \
	  CXX_VER=$$($(CXX) -dumpversion | cut -d. -f1); \
	  if [ "$$CXX_VER" -lt 10 ]; then \
	    echo ""; \
	    echo "  WARN: $(CXX) version $$CXX_VER is too old (need >= 10 for C++20 / real_writer)"; \
	    echo "    apt:  sudo apt-get install -y g++-10  (or g++-12 on Ubuntu 22.04)"; \
	    echo "    then: make CXX=g++-12 real_writer"; \
	    touch /tmp/.missing_deps; \
	  fi; \
	fi
	@if [ -f /tmp/.missing_deps ]; then \
	  echo ""; \
	  echo "One or more dependencies are missing. Install them then re-run make."; \
	  echo "  Quick fix (Debian/Ubuntu):"; \
	  echo "    make deps   # installs all apt packages above (requires sudo)"; \
	  rm -f /tmp/.missing_deps; \
	  exit 1; \
	fi
	@echo "All dependencies found."

# parquet_writer.cpp = real_writer.cpp + mqtt_ring (high-speed SPSC ring buffer)
# This is the production binary for all deployments (gx10, evelyn34, k8s).
# real_writer and writer are kept for reference / backward compatibility.

PARQUET_WRITER_FLAGS := -std=c++20 -O2 -Wall -Wextra \
              -DTEST_PYTHON_BIN=\"$(TEST_PYTHON)\" \
              $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(YAML_CFLAGS) $(SIMDJSON_CFLAGS)

.PHONY: all clean deps check-deps test test_real test_parquet kpi kpi_real kpi_parquet

all: check-deps parquet_writer real_writer

parquet_writer: check-deps parquet_writer.cpp mqtt_ring.h
	$(CXX) $(ALL_CFLAGS) -std=c++20 parquet_writer.cpp -o $@ $(ALL_LIBS)

real_writer: real_writer.cpp check-deps
	$(CXX) $(ALL_CFLAGS) -std=c++20 $< -o $@ $(ALL_LIBS)

tests/test_writer: tests/test_writer.cpp writer.cpp tests/test_runner.h
	$(CXX) $(TEST_FLAGS) -I. tests/test_writer.cpp -o $@ $(ALL_LIBS)

tests/test_real_writer: tests/test_real_writer.cpp real_writer.cpp tests/test_runner.h
	$(CXX) $(REAL_TEST_FLAGS) -I. tests/test_real_writer.cpp -o $@ $(ALL_LIBS)

tests/test_parquet_writer: tests/test_real_writer.cpp parquet_writer.cpp mqtt_ring.h tests/test_runner.h
	$(CXX) $(PARQUET_WRITER_FLAGS) -DPARQUET_WRITER_NO_MAIN -DREAL_WRITER_NO_MAIN -I. \
	  tests/test_real_writer.cpp -o $@ $(ALL_LIBS)

tests/kpi_writer: tests/kpi_writer.cpp writer.cpp
	$(CXX) $(TEST_FLAGS) -I. tests/kpi_writer.cpp -o $@ $(ALL_LIBS)

tests/kpi_real_writer: tests/kpi_real_writer.cpp real_writer.cpp tests/test_runner.h
	$(CXX) $(REAL_TEST_FLAGS) -I. tests/kpi_real_writer.cpp -o $@ $(ALL_LIBS)

tests/kpi_parquet_writer: tests/kpi_real_writer.cpp parquet_writer.cpp mqtt_ring.h tests/test_runner.h
	$(CXX) $(PARQUET_WRITER_FLAGS) -DPARQUET_WRITER_NO_MAIN -DREAL_WRITER_NO_MAIN -I. \
	  tests/kpi_real_writer.cpp -o $@ $(ALL_LIBS)

test: tests/test_writer
	./tests/test_writer

test_real: tests/test_real_writer
	./tests/test_real_writer

test_parquet: tests/test_parquet_writer
	./tests/test_parquet_writer

kpi: tests/kpi_writer
	./tests/kpi_writer

kpi_real: tests/kpi_real_writer
	./tests/kpi_real_writer

kpi_parquet: tests/kpi_parquet_writer
	./tests/kpi_parquet_writer

# Install missing dev packages (requires sudo)
# Arrow/Parquet are not always in the default apt repo — this covers Ubuntu 22.04+.
# On older distros or Alpine/scratch images, see: https://arrow.apache.org/install/
deps:
	@echo "Installing apt dependencies..."
	sudo apt-get install -y \
	  build-essential \
	  libarrow-dev libparquet-dev \
	  libmosquitto-dev \
	  libyaml-cpp-dev \
	  libsimdjson-dev
	@echo ""
	@echo "If libarrow-dev was not found, add the Apache Arrow apt repo first:"
	@echo "  wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-\$$(lsb_release -cs).deb"
	@echo "  sudo apt install ./apache-arrow-apt-source-latest-*.deb"
	@echo "  sudo apt update && make deps"

clean:
	rm -f parquet_writer real_writer \
	  tests/test_writer tests/test_real_writer tests/test_parquet_writer \
	  tests/kpi_writer tests/kpi_real_writer tests/kpi_parquet_writer
