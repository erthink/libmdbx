all check test dist: Makefile
	@CC=$(CC) CXX=$(CXX) `which gmake || which gnumake || echo 'echo "GNU Make is required"'` $(MAKEFLAGS) -f GNUmakefile $@

