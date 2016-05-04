/*****************************************************************************
 * Properly compiler/memory/coherence barriers
 * in the most portable way for ReOpenLDAP project.
 *
 * Feedback and comments are welcome.
 * https://gist.github.com/leo-yuriev/ba186a6bf5cf3a27bae7                   */

#if defined(__mips) && defined(__linux)
	/* Only MIPS has explicit cache control */
#	include <asm/cachectl.h>
#endif

#if defined(__GNUC__) || defined(__clang__)
#	define MDBX_INLINE __inline
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
#	include <intrin.h>
#	if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#		pragma intrinsic(__mf)
#	elif defined(__i386__) || defined(__x86_64__)
#		pragma intrinsic(_mm_mfence)
#	endif
#	define MDBX_INLINE __inline
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
#	include <mbarrier.h>
#	define MDBX_INLINE inline
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) \
	&& (defined(HP_IA64) || defined(__ia64))
#	include <machine/sys/inline.h>
#	define MDBX_INLINE
#elif defined(__IBMC__) && defined(__powerpc)
#	include <atomic.h>
#	define MDBX_INLINE
#elif defined(_AIX)
#	include <builtins.h>
#	include <sys/atomic_op.h>
#	define MDBX_INLINE
#elif (defined(__osf__) && defined(__DECC)) || defined(__alpha)
#	include <machine/builtins.h>
#	include <c_asm.h>
#	define MDBX_INLINE
#elif defined(__MWERKS__)
	/* CodeWarrior - troubles ? */
#	pragma gcc_extensions
#	define MDBX_INLINE
#elif defined(__SNC__)
	/* Sony PS3 - troubles ? */
#	define MDBX_INLINE
#else
#	define MDBX_INLINE
#endif

#if defined(__i386__) || defined(__x86_64__) \
	|| defined(_M_AMD64) || defined(_M_IX86) \
	|| defined(__i386) || defined(__amd64) \
	|| defined(i386) || defined(__x86_64) \
	|| defined(_AMD64_) || defined(_M_X64)
#	define MDB_CACHE_IS_COHERENT 1
#elif defined(__hppa) || defined(__hppa__)
#	define MDB_CACHE_IS_COHERENT 1
#endif

#ifndef MDB_CACHE_IS_COHERENT
#	define MDB_CACHE_IS_COHERENT 0
#endif

#define MDBX_BARRIER_COMPILER 0
#define MDBX_BARRIER_MEMORY 1

static MDBX_INLINE void mdbx_barrier(int type) {
#if defined(__clang__)
	__asm__ __volatile__ ("" ::: "memory");
	if (type > MDBX_BARRIER_COMPILER)
#	if __has_extension(c_atomic) || __has_extension(cxx_atomic)
		__c11_atomic_thread_fence(__ATOMIC_SEQ_CST);
#	else
		__sync_synchronize();
#	endif
#elif defined(__GNUC__)
	__asm__ __volatile__ ("" ::: "memory");
	if (type > MDBX_BARRIER_COMPILER)
#	if defined(__ATOMIC_SEQ_CST)
		__atomic_thread_fence(__ATOMIC_SEQ_CST);
#	else
		__sync_synchronize();
#	endif
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
	__memory_barrier();
	if (type > MDBX_BARRIER_COMPILER)
#	if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
		__mf();
#	elif defined(__i386__) || defined(__x86_64__)
		_mm_mfence();
#	else
#		error "Unknown target for Intel Compiler, please report to us."
#	endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
	__compiler_barrier();
	if (type > MDBX_BARRIER_COMPILER)
		__machine_rw_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) \
	&& (defined(HP_IA64) || defined(__ia64))
	_Asm_sched_fence(/* LY: no-arg meaning 'all expect ALU', e.g. 0x3D3D */);
	if (type > MDBX_BARRIER_COMPILER)
		_Asm_mf();
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) \
	|| defined(__ppc64__) || defined(__powerpc64__)
	__fence();
	if (type > MDBX_BARRIER_COMPILER)
		__lwsync();
#elif (defined(__osf__) && defined(__DECC)) || defined(__alpha)
	__PAL_DRAINA(); /* LY: excessive ? */
	__MB();
#else
#	error "Could not guess the kind of compiler, please report to us."
#endif
}

#define mdbx_compiler_barrier() \
	mdbx_barrier(MDBX_BARRIER_COMPILER)
#define mdbx_memory_barrier() \
	mdbx_barrier(MDBX_BARRIER_MEMORY)
#define mdbx_coherent_barrier() \
	mdbx_barrier(MDB_CACHE_IS_COHERENT ? MDBX_BARRIER_COMPILER : MDBX_BARRIER_MEMORY)

static MDBX_INLINE void mdb_invalidate_cache(void *addr, int nbytes) {
	mdbx_coherent_barrier();
#if defined(__mips) && defined(__linux)
	/* MIPS has cache coherency issues.
	 * Note: for any nbytes >= on-chip cache size, entire is flushed. */
	cacheflush(addr, nbytes, DCACHE);
#elif defined(_M_MRX000) || defined(_MIPS_)
#	error "Sorry, cacheflush() for MIPS not implemented"
#else
	/* LY: assume no mmap/dcache issues. */
	(void) addr;
	(void) nbytes;
#endif
}

