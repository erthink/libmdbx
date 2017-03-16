/** A generic unsigned ID number. These were entryIDs in back-bdb.
 *	Preferably it should have the same size as a pointer.
 */
typedef size_t MDB_ID;

/** An IDL is an ID List, a sorted array of IDs. The first
 * element of the array is a counter for how many actual
 * IDs are in the list. In the original back-bdb code, IDLs are
 * sorted in ascending order. For libmdb IDLs are sorted in
 * descending order.
 */
typedef MDB_ID *MDB_IDL;

/* IDL sizes - likely should be even bigger
 *   limiting factors: sizeof(ID), thread stack size
 */
#define MDB_IDL_LOGN 16 /* DB_SIZE is 2^16, UM_SIZE is 2^17 */
#define MDB_IDL_DB_SIZE (1 << MDB_IDL_LOGN)
#define MDB_IDL_UM_SIZE (1 << (MDB_IDL_LOGN + 1))

#define MDB_IDL_DB_MAX (MDB_IDL_DB_SIZE - 1)
#define MDB_IDL_UM_MAX (MDB_IDL_UM_SIZE - 1)

#define MDB_IDL_SIZEOF(ids) (((ids)[0] + 1) * sizeof(MDB_ID))
#define MDB_IDL_IS_ZERO(ids) ((ids)[0] == 0)
#define MDB_IDL_CPY(dst, src) (memcpy(dst, src, MDB_IDL_SIZEOF(src)))
#define MDB_IDL_FIRST(ids) ((ids)[1])
#define MDB_IDL_LAST(ids) ((ids)[(ids)[0]])

/** Current max length of an #mdbx_midl_alloc()ed IDL */
#define MDB_IDL_ALLOCLEN(ids) ((ids)[-1])

/** Append ID to IDL. The IDL must be big enough. */
#define mdbx_midl_xappend(idl, id)                                             \
  do {                                                                         \
    MDB_ID *xidl = (idl), xlen = ++(xidl[0]);                                  \
    xidl[xlen] = (id);                                                         \
  } while (0)

/** An ID2 is an ID/pointer pair.
 */
typedef struct MDB_ID2 {
  MDB_ID mid; /**< The ID */
  void *mptr; /**< The pointer */
} MDB_ID2;

/** An ID2L is an ID2 List, a sorted array of ID2s.
 * The first element's \b mid member is a count of how many actual
 * elements are in the array. The \b mptr member of the first element is
 * unused.
 * The array is sorted in ascending order by \b mid.
 */
typedef MDB_ID2 *MDB_ID2L;
