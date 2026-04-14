CXX      := g++
CXXFLAGS := -std=c++20 -O2 -Wall -Wextra

# Arrow / Parquet
ARROW_CFLAGS := $(shell pkg-config --cflags arrow parquet 2>/dev/null)
ARROW_LIBS   := $(shell pkg-config --libs   arrow parquet 2>/dev/null || echo "-lparquet -larrow")

# mosquitto
MOSQ_CFLAGS  := $(shell pkg-config --cflags libmosquitto 2>/dev/null)
MOSQ_LIBS    := $(shell pkg-config --libs   libmosquitto 2>/dev/null || echo "-lmosquitto")

# yaml-cpp
YAML_CFLAGS  := $(shell pkg-config --cflags yaml-cpp 2>/dev/null)
YAML_LIBS    := $(shell pkg-config --libs   yaml-cpp 2>/dev/null || echo "-lyaml-cpp")

# simdjson
SIMDJSON_CFLAGS := $(shell pkg-config --cflags simdjson 2>/dev/null)
SIMDJSON_LIBS   := $(shell pkg-config --libs   simdjson 2>/dev/null || echo "-lsimdjson")

ALL_CFLAGS := $(CXXFLAGS) $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(YAML_CFLAGS) $(SIMDJSON_CFLAGS)
ALL_LIBS   := $(ARROW_LIBS) $(MOSQ_LIBS) $(YAML_LIBS) $(SIMDJSON_LIBS)

TEST_PYTHON := $(shell \
  ls $(HOME)/data_server/.venv/bin/python3 2>/dev/null || \
  ls $(HOME)/work/gen-ai/data_server/.venv/bin/python3 2>/dev/null || \
  echo python3)

TEST_FLAGS := $(CXXFLAGS) \
              -DPARQUET_WRITER_NO_MAIN \
              -DTEST_PYTHON_BIN=\"$(TEST_PYTHON)\" \
              $(ARROW_CFLAGS) $(MOSQ_CFLAGS) $(YAML_CFLAGS) $(SIMDJSON_CFLAGS)

# --------------------------------------------------------------------------
# Dependency checker
# --------------------------------------------------------------------------
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
	    echo "  WARN: $(CXX) version $$CXX_VER is too old (need >= 10 for C++20)"; \
	    echo "    apt:  sudo apt-get install -y g++-12"; \
	    echo "    then: make CXX=g++-12"; \
	    touch /tmp/.missing_deps; \
	  fi; \
	fi
	@if [ -f /tmp/.missing_deps ]; then \
	  echo ""; \
	  echo "One or more dependencies are missing. Install them then re-run make."; \
	  echo "  Quick fix (Debian/Ubuntu):  make deps"; \
	  rm -f /tmp/.missing_deps; \
	  exit 1; \
	fi
	@echo "All dependencies found."

# --------------------------------------------------------------------------
# Targets
# --------------------------------------------------------------------------

.PHONY: all clean deps test kpi

all: check-deps parquet_writer

parquet_writer: check-deps parquet_writer.cpp mqtt_ring.h
	$(CXX) $(ALL_CFLAGS) parquet_writer.cpp -o $@ $(ALL_LIBS)

tests/test_parquet_writer: tests/test_parquet_writer.cpp parquet_writer.cpp mqtt_ring.h tests/test_runner.h
	$(CXX) $(TEST_FLAGS) -I. tests/test_parquet_writer.cpp -o $@ $(ALL_LIBS)

tests/kpi_parquet_writer: tests/kpi_parquet_writer.cpp parquet_writer.cpp mqtt_ring.h tests/test_runner.h
	$(CXX) $(TEST_FLAGS) -I. tests/kpi_parquet_writer.cpp -o $@ $(ALL_LIBS)

test: tests/test_parquet_writer
	./tests/test_parquet_writer

kpi: tests/kpi_parquet_writer
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
	rm -f parquet_writer tests/test_parquet_writer tests/kpi_parquet_writer
