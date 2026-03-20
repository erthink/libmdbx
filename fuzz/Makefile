FUZZ_DB_SRCS := fuzz_raw_db_format.c logger.c mode_desc.c
FUZZ_DB_OBJS := $(addprefix obj/fuzz/,$(FUZZ_DB_SRCS:.c=.o))

DB_GEN_SRCS := utils/raw_db_gen.c mode_desc.c
DB_GEN_OBJS := $(addprefix obj/dbgen/,$(DB_GEN_SRCS:.c=.o))

CC := clang
CXX := clang++
CFLAGS := -Wall -Wextra
CXXFLAGS := -Wall -Wextra
LIBFUZZER := -fsanitize=fuzzer,address -fno-omit-frame-pointer
INCLUDE := -I. -I..

LIBMDBX_NORMAL := obj/libmdbx.normal.a
LIBMDBX_FUZZ := obj/libmdbx.fuzz.a

all: fuzz_raw_db_format db_seed_gen

$(LIBMDBX_NORMAL):
	@mkdir -p $(dir $@)
	$(MAKE) -C .. clean
	$(MAKE) -C .. libmdbx.a \
		CC="$(CC)" CFLAGS="$(CFLAGS)" \
		CXX="$(CXX)" CXXFLAGS="$(CXXFLAGS)"
	cp ../libmdbx.a $@

$(LIBMDBX_FUZZ):
	@mkdir -p $(dir $@)
	$(MAKE) -C .. clean
	$(MAKE) -C .. libmdbx.a \
		CC="$(CC)" CFLAGS="$(CFLAGS) $(LIBFUZZER)" \
		CXX="$(CXX)" CXXFLAGS="$(CXXFLAGS) $(LIBFUZZER)"
	cp ../libmdbx.a $@

fuzz_raw_db_format: CFLAGS += $(LIBFUZZER)
fuzz_raw_db_format: CXXFLAGS += $(LIBFUZZER)
fuzz_raw_db_format: $(LIBMDBX_FUZZ) $(FUZZ_DB_OBJS)
	$(CC) $(CFLAGS) -o $@ $(FUZZ_DB_OBJS) $(LIBMDBX_FUZZ)

db_seed_gen: $(LIBMDBX_NORMAL) $(DB_GEN_OBJS)
	$(CC) $(CFLAGS) -o $@ $(DB_GEN_OBJS) $(LIBMDBX_NORMAL)

obj/fuzz/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

obj/dbgen/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

clean:
	rm -rf obj fuzz_raw_db_format db_seed_gen

.PHONY: all clean
