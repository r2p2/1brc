CC := gcc
RM := rm -f

WARN := -Wall -Wextra -Wpedantic
BUILD ?= debug
CFLAGS := -MMD -MP
SANITIZER := -fsanitize=address
# SANITIZER := 

ifeq ($(BUILD),debug)
	CFLAGS := $(CFLAGS) -g -O0 $(SANITIZER) $(WARN)
	LDFLAGS := $(SANITIZER)
	BUILD_DIR := build/debug
else ifeq ($(BUILD),release) 
	CFLAGS := $(CFLAGS) -O3 -march=native -DNDEBUG $(WARN)
	LDFLAGS := -s
	BUILD_DIR := build/release
else
	$(error Unknown build type: $(build))
endif

SRCS := $(wildcard src/*.c)
OBJS := $(SRCS:src/%.c=$(BUILD_DIR)/%.o)
TARGET := $(BUILD_DIR)/1blc

$(BUILD_DIR)/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

.PHYONY: all debug release clean

all: $(TARGET)

debug:
	$(MAKE) BUILD=debug

release:
	$(MAKE) BUILD=release

clean:
	$(RM) -r build

test: release
	./run-test.sh

compile_commands:
	bear -- $(MAKE)

-include $(OBJS:.o=.d)
