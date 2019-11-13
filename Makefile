all install mdbx tools strip clean test check dist test-singleprocess test-fault memcheck test-valgrind cross-gcc cross-qemu bench bench-quartet clean-bench:
	@CC=$(CC) \
	CXX=`if test -n "$(CXX)" && which "$(CXX)" > /dev/null; then echo "$(CXX)"; elif test -n "$(CCC)" && which "$(CCC)" > /dev/null; then echo "$(CCC)"; else echo "c++"; fi` \
	`which gmake || which gnumake || echo 'echo "GNU Make is required"; exit 2;'` \
		$(MAKEFLAGS) -f GNUmakefile $@
