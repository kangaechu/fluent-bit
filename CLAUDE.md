# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fluent Bit is a lightweight, high-performance telemetry agent for collecting, processing, and forwarding logs, metrics, and traces. It's a CNCF graduated project written primarily in C with embedded libraries for minimal dependencies.

## Build System and Development Commands

### Prerequisites
- CMake >= 3.12
- Flex & Bison (or win_flex_bison on Windows)
- OpenSSL development headers
- YAML development headers

### Building from Source
```bash
# Standard build
cd build
cmake ..
make

# Development build with debugging
cmake -DFLB_DEV=On ../
make

# Enable tests
cmake -DFLB_DEV=On -DFLB_TESTS_RUNTIME=On -DFLB_TESTS_INTERNAL=On ../
make
```

### Testing
- Unit tests: `make test` or `ctest --output-on-failure`
- Individual tests: Run executables from `build/bin/` (e.g., `./bin/flb-it-sds`)
- Parallel tests: `ctest -j${NUM_PROC}`
- Runtime tests cover plugins, internal tests cover core libraries

### Windows Development
- Use Visual Studio 2022 or later
- Install dependencies via vcpkg: `vcpkg install --triplet x64-windows-static`
- Generate solution: `cmake -G "Visual Studio 17 2022" -DCMAKE_TOOLCHAIN_FILE=...`
- Build: `cmake --build . --parallel 4`

### Memory Analysis
```bash
# Valgrind build and usage
cmake -DFLB_DEV=On -DFLB_VALGRIND=On ../
make
valgrind ./bin/fluent-bit [args]
```

## Architecture and Code Structure

### Core Components
- **Engine** (`src/flb_engine.c`): Main event loop and coroutine scheduler
- **Input Plugins** (`plugins/in_*/`): Data collection (70+ built-in plugins)
- **Filter Plugins** (`plugins/filter_*/`): Data transformation and enrichment
- **Output Plugins** (`plugins/out_*/`): Data delivery to external services
- **Core Libraries** (`src/`): Memory management, networking, parsing, etc.

### Plugin Architecture
- Plugins are shared objects loaded via dlopen/dlsym
- Each plugin implements specific callbacks: `cb_init`, `cb_collect`/`cb_filter`/`cb_flush`, `cb_exit`
- Config maps provide type-safe configuration parsing with validation
- Use `flb_output_config_map_set()` to populate plugin context from config

### Concurrency Model
- Uses cooperative coroutines, not threads
- Only one coroutine active at a time - no mutex/synchronization needed
- Async I/O operations yield execution to the event loop
- Filter plugins do NOT support coroutines - disable async: `upstream->flags &= ~(FLB_IO_ASYNC)`
- Output plugins use coroutines - be careful with shared context state across async calls

### Memory Management
Always use Fluent Bit's memory functions:
- `flb_malloc()`, `flb_calloc()`, `flb_realloc()`, `flb_free()`
- Many types have specialized create/destroy functions (e.g., `flb_sds_create()`)

### String Handling
- Use SDS (Simple Dynamic Strings) library: `flb_sds.h`
- SDS strings are C-compatible but provide dynamic resizing and length tracking
- Always use SDS for string processing code

### HTTP Client Usage
```c
// Create upstream connection
upstream = flb_upstream_create(config, host, port, FLB_IO_TCP, NULL);
u_conn = flb_upstream_conn_get(upstream);

// Create HTTP client and make request
client = flb_http_client(u_conn, FLB_HTTP_GET, path, NULL, 0, host, port, NULL, 0);
ret = flb_http_do(client, &b_sent);

// Always cleanup resources
flb_http_client_destroy(client);
flb_upstream_conn_release(u_conn);
flb_upstream_destroy(upstream);
```

### Message Pack Processing
- Internal data format is msgpack
- Records are arrays: `[timestamp, map]`
- Use msgpack-c library functions for packing/unpacking
- See `plugins/filter_record_modifier/` for examples of record manipulation

## Development Environment Options

### Devcontainer
```bash
docker run --name devcontainer-fluent-bit \
  --volume $PWD/:/workspaces/fluent-bit \
  --user $UID:$GID --tty --detach \
  fluent/fluent-bit:latest-debug
```

### Vagrant
```bash
vagrant up
vagrant ssh
cd build && cmake .. && make
```

## Code Quality and Standards

### Style Guidelines
- Follow Apache C style guidelines
- Line length: max 90 characters
- Always use braces for conditionals/loops
- Indentation: 4 spaces (no tabs)
- Opening brace on same line as statement

### Security Practices
- Never expose or log secrets/keys
- Never commit secrets to repository
- Use Fluent Bit's crypto functions for security operations
- Enable stack protection for WASM builds when needed

### Plugin Development Best Practices
- Use config maps for type-safe configuration
- Implement proper error handling and cleanup
- Test plugins with various input data
- Consider memory usage and performance impact
- Filter plugins: disable async I/O for HTTP requests
- Output plugins: be aware of coroutine context sharing issues

## File Structure Notes
- `src/`: Core Fluent Bit implementation
- `plugins/`: Input, filter, and output plugins
- `include/fluent-bit/`: Public header files
- `lib/`: Embedded external libraries (msgpack, monkey, etc.)
- `tests/`: Unit and integration tests
- `conf/`: Sample configuration files
- `cmake/`: Build system configuration