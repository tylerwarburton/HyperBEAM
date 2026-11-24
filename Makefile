.PHONY: compile buildinfo clean-generated preloaded-store wamrc wasm-patch-instrument wasm-patch-instrument-test

compile:
	rebar3 compile

WAMR_VERSION = 2.2.0
WAMR_DIR = _build/wamr
# wasm-patch@1.0 compiles AOT on the node with wamrc from the SAME WAMR release
# as the runtime. The release binary is built on Ubuntu 22.04 (glibc 2.35) and
# runs on the node image (Debian bookworm, glibc 2.36); it needs libz, libzstd
# and libstdc++ at run time. x86_64 Linux only, like the nodes.
WAMRC_URL = https://github.com/bytecodealliance/wasm-micro-runtime/releases/download/WAMR-$(WAMR_VERSION)/wamrc-$(WAMR_VERSION)-x86_64-ubuntu-22.04.tar.gz
WAMRC_SHA256 = b6207bd4c98835cfdaaa92ead83de7eeff5b58c07d4ee209ec900dce502246d1
WAMRC_BIN = priv/wamrc
WPI_DIR = native/wasm_patch_instrument
WPI_BIN = priv/wasm-patch-instrument
BUILD_SOURCE = $(shell git rev-parse HEAD)
BUILD_SOURCE_SHORT = $(shell git rev-parse --short HEAD)

GENESIS_WASM_BRANCH = feat/hb-unit
GENESIS_WASM_REPO = https://github.com/permaweb/ao.git
GENESIS_WASM_SERVER_DIR = _build/genesis_wasm/genesis-wasm-server

HYPERBUDDY_UI_REPO = https://github.com/permaweb/hb-explorer
HYPERBUDDY_UI_PACKAGE_JSON = https://raw.githubusercontent.com/permaweb/hb-explorer/main/package.json
HYPERBUDDY_UI_TARGET = src/html/hyperbuddy@1.0/bundle.js
ARWEAVE_GATEWAY = https://arweave.net
PRELOADED_INPUTS = \
	scripts/build-preloaded-store.escript \
	rebar.config \
	rebar.lock \
	include \
	src/preloaded \
	src/forge \
	src/core/device \
	src/core/include \
	specs
CLEAN_GENERATED = \
	priv \
	$(WAMR_DIR) \
	_build/wamrc \
	_build/genesis_wasm \
	_build/preloaded-store \
	_build/device-test-store \
	_build/device-implementations \
	_build/hb_buildinfo.hrl \
	_build/hb_buildinfo.hrl.* \
	_build/hb_preloaded_index.hrl \
	_build/hb_preloaded_index.hrl.* \
	native/hb_beamr/*.o \
	native/hb_beamr/*.d \
	native/hb_keccak/*.o \
	native/hb_keccak/*.d

ifdef HB_DEBUG
	WAMR_FLAGS = -DWAMR_ENABLE_LOG=1 -DWAMR_BUILD_DUMP_CALL_STACK=1 -DCMAKE_BUILD_TYPE=Debug
else
	WAMR_FLAGS = -DCMAKE_BUILD_TYPE=Release
endif

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
    WAMR_BUILD_PLATFORM = darwin
    ifeq ($(UNAME_M),arm64)
        WAMR_BUILD_TARGET = AARCH64
    else
        WAMR_BUILD_TARGET = X86_64
    endif
else
    WAMR_BUILD_PLATFORM = linux
    WAMR_BUILD_TARGET = X86_64
endif

wamr: $(WAMR_DIR)/lib/libvmlib.a

buildinfo:
	@mkdir -p priv
	@tmp=$$(mktemp priv/hb_buildinfo.XXXXXX) && \
		printf '%s\n' \
			'#{' \
			'    <<"source">> => <<"$(BUILD_SOURCE)">>,' \
			'    <<"source-short">> => <<"$(BUILD_SOURCE_SHORT)">>,' \
			'    <<"build-time">> => '$$(date +%s) \
			'}.' \
			> "$$tmp" && \
		mv "$$tmp" priv/hb_buildinfo

preloaded-store:
	@if [ ! -f _build/preloaded-store/data.mdb ] || \
		[ -n "$$(find $(PRELOADED_INPUTS) -type f -newer _build/preloaded-store/data.mdb -print -quit)" ] || \
		{ key=$${HB_KEY:-hyperbeam-key.json}; [ ! -f "$$key" ] || [ "$$key" -nt _build/preloaded-store/data.mdb ]; }; then \
		scripts/build-preloaded-store.escript; \
	fi

clean-generated:
	rm -rf $(CLEAN_GENERATED)

debug: debug-clean $(WAMR_DIR)
	HB_DEBUG=1 make $(WAMR_DIR)/lib/libvmlib.a
	CFLAGS="-DHB_DEBUG=1" rebar3 compile

debug-clean:
	rm -rf priv $(WAMR_DIR)

# Clone the WAMR repository at our target release
$(WAMR_DIR):
	git clone \
		https://github.com/bytecodealliance/wasm-micro-runtime.git \
		$(WAMR_DIR) \
		-b WAMR-$(WAMR_VERSION) \
		--single-branch

$(WAMR_DIR)/lib/libvmlib.a: $(WAMR_DIR)
	@if ! grep -Fq 'tbl_inst->is_table64 = 1;' ./_build/wamr/core/iwasm/aot/aot_runtime.c; then \
		awk 'NR == 742 { print; print "tbl_inst->is_table64 = 1;"; next } { print }' \
			./_build/wamr/core/iwasm/aot/aot_runtime.c > ./_build/wamr/core/iwasm/aot/aot_runtime.c.tmp && \
		mv ./_build/wamr/core/iwasm/aot/aot_runtime.c.tmp ./_build/wamr/core/iwasm/aot/aot_runtime.c; \
	fi; \
	sed -i '/wasm_interp_call_func_native(module, exec_env, cur_func,/,/wasm_copy_exception(module, NULL)/ s/linear_mem_size = get_linear_mem_size();/linear_mem_size = GET_LINEAR_MEMORY_SIZE(memory);/' \
		./_build/wamr/core/iwasm/interpreter/wasm_interp_classic.c && \
	! sed -n '/wasm_interp_call_func_native(module, exec_env, cur_func,/,/wasm_copy_exception(module, NULL)/p' \
		./_build/wamr/core/iwasm/interpreter/wasm_interp_classic.c | grep -q 'get_linear_mem_size()' && \
	cmake \
		$(WAMR_FLAGS) \
		-S $(WAMR_DIR) \
		-B $(WAMR_DIR)/lib \
		-DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
		-DWAMR_BUILD_TARGET=$(WAMR_BUILD_TARGET) \
		-DWAMR_BUILD_PLATFORM=$(WAMR_BUILD_PLATFORM) \
		-DWAMR_BUILD_MEMORY64=1 \
		-DWAMR_DISABLE_HW_BOUND_CHECK=1 \
		-DWAMR_BUILD_EXCE_HANDLING=1 \
		-DWAMR_BUILD_SHARED_MEMORY=0 \
		-DWAMR_BUILD_AOT=1 \
		-DWAMR_BUILD_LIBC_WASI=0 \
		-DWAMR_BUILD_FAST_INTERP=0 \
		-DWAMR_BUILD_INTERP=1 \
		-DWAMR_BUILD_JIT=0 \
		-DWAMR_BUILD_FAST_JIT=0 \
        -DWAMR_BUILD_DEBUG_AOT=1 \
        -DWAMR_BUILD_TAIL_CALL=1 \
        -DWAMR_BUILD_AOT_STACK_FRAME=1 \
        -DWAMR_BUILD_MEMORY_PROFILING=1 \
        -DWAMR_BUILD_DUMP_CALL_STACK=1
	make -C $(WAMR_DIR)/lib -j8

# The second patch above is a WAMR 2.2.0 classic-interpreter fix: after a
# native call returns, the caller frame refreshed its cached linear memory size
# from get_linear_mem_size(), which without THREAD_MGR is that same cached
# value. Memory grown inside the native's nested call (wasm-patch's invoke_*
# natives re-enter wasm through call_indirect) then read as out of bounds in
# the caller. AOT is unaffected. Runtime flags are unchanged.

wamrc: $(WAMRC_BIN)

$(WAMRC_BIN):
	@if [ "$(UNAME_S)/$(UNAME_M)" != "Linux/x86_64" ]; then \
		echo "wamrc: no pinned release for $(UNAME_S)/$(UNAME_M)"; exit 1; fi
	@mkdir -p _build/wamrc priv
	curl -fsSL -o _build/wamrc/wamrc.tar.gz $(WAMRC_URL)
	echo "$(WAMRC_SHA256)  _build/wamrc/wamrc.tar.gz" | sha256sum -c -
	tar xzf _build/wamrc/wamrc.tar.gz -C _build/wamrc wamrc
	test "$$(_build/wamrc/wamrc --version)" = "wamrc $(WAMR_VERSION)"
	install -m 0755 _build/wamrc/wamrc $@

# The wasm-patch@1.0 validation and fuel/depth pass, built with the toolchain
# pinned in $(WPI_DIR)/rust-toolchain.toml (rustup reads it from the cwd).
wasm-patch-instrument:
	cd $(WPI_DIR) && cargo build --release --locked
	@mkdir -p priv
	install -m 0755 $(WPI_DIR)/target/release/wasm-patch-instrument $(WPI_BIN)
	$(WPI_BIN) --version

# Unit tests, then interpreter-vs-AOT equivalence on the node's own runtime build.
wasm-patch-instrument-test: wamr wamrc wasm-patch-instrument
	cd $(WPI_DIR) && cargo test --release --locked
	$(WPI_DIR)/tests/wamr/run.sh $(CURDIR)/$(WAMR_DIR) $(CURDIR)/$(WAMRC_BIN) $(CURDIR)/$(WPI_BIN)

clean:
	rebar3 clean

# Add a new target to print the library path
print-lib-path:
	@echo $(CURDIR)/lib/libvmlib.a

$(GENESIS_WASM_SERVER_DIR):
	mkdir -p $(GENESIS_WASM_SERVER_DIR)
	@echo "Cloning genesis-wasm repository..." && \
        tmp_dir=$$(mktemp -d) && \
        git clone --depth=1 -b $(GENESIS_WASM_BRANCH) $(GENESIS_WASM_REPO) $$tmp_dir && \
        mkdir -p $(GENESIS_WASM_SERVER_DIR) && \
        cp -r $$tmp_dir/servers/cu/* $(GENESIS_WASM_SERVER_DIR) && \
        rm -rf $$tmp_dir && \
        echo "Extracted servers/genesis-wasm to $(GENESIS_WASM_SERVER_DIR)"

# Set up genesis-wasm@1.0 environment
setup-genesis-wasm: $(GENESIS_WASM_SERVER_DIR)
	@cp native/genesis-wasm/launch-monitored.sh $(GENESIS_WASM_SERVER_DIR) && \
	if ! command -v node > /dev/null; then \
		echo "Error: Node.js is not installed. Please install Node.js before continuing."; \
		echo "For Ubuntu/Debian, you can install it with:"; \
		echo "  curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - && \\"; \
		echo "  apt-get install -y nodejs=22.16.0-1nodesource1 --allow-downgrades && \\"; \
		echo "  node -v && npm -v"; \
		exit 1; \
	fi
	@cd $(GENESIS_WASM_SERVER_DIR) && npm install > /dev/null 2>&1 && \
		echo "Installed genesis-wasm@1.0 server."

# Update hyperbuddy-ui from remote bundle
update-hyperbuddy-ui:
	@echo "Fetching package.json from $(HYPERBUDDY_UI_REPO)..." && \
	TX_ID=$$(curl -s "$(HYPERBUDDY_UI_PACKAGE_JSON)" | grep -o '"bundle"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4) && \
	if [ -z "$$TX_ID" ]; then \
		echo "Error: Could not find 'bundle' field in package.json"; \
		exit 1; \
	fi && \
	echo "Found transaction ID: $$TX_ID" && \
	if [ -f "$(HYPERBUDDY_UI_TARGET)" ]; then \
		rm "$(HYPERBUDDY_UI_TARGET)" && \
		echo "Removed existing bundle.js"; \
	fi && \
	echo "Downloading source code from Arweave..." && \
	curl -sL "$(ARWEAVE_GATEWAY)/$$TX_ID" -o "$(HYPERBUDDY_UI_TARGET)" && \
	echo "Successfully updated $(HYPERBUDDY_UI_TARGET)"
