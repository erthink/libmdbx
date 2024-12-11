/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

typedef union osal_srwlock {
  __anonymous_struct_extension__ struct {
    long volatile readerCount;
    long volatile writerCount;
  };
  RTL_SRWLOCK native;
} osal_srwlock_t;

typedef void(WINAPI *osal_srwlock_t_function)(osal_srwlock_t *);

#if _WIN32_WINNT < 0x0600 /* prior to Windows Vista */
typedef enum _FILE_INFO_BY_HANDLE_CLASS {
  FileBasicInfo,
  FileStandardInfo,
  FileNameInfo,
  FileRenameInfo,
  FileDispositionInfo,
  FileAllocationInfo,
  FileEndOfFileInfo,
  FileStreamInfo,
  FileCompressionInfo,
  FileAttributeTagInfo,
  FileIdBothDirectoryInfo,
  FileIdBothDirectoryRestartInfo,
  FileIoPriorityHintInfo,
  FileRemoteProtocolInfo,
  MaximumFileInfoByHandleClass
} FILE_INFO_BY_HANDLE_CLASS,
    *PFILE_INFO_BY_HANDLE_CLASS;

typedef struct _FILE_END_OF_FILE_INFO {
  LARGE_INTEGER EndOfFile;
} FILE_END_OF_FILE_INFO, *PFILE_END_OF_FILE_INFO;

#define REMOTE_PROTOCOL_INFO_FLAG_LOOPBACK 0x00000001
#define REMOTE_PROTOCOL_INFO_FLAG_OFFLINE 0x00000002

typedef struct _FILE_REMOTE_PROTOCOL_INFO {
  USHORT StructureVersion;
  USHORT StructureSize;
  DWORD Protocol;
  USHORT ProtocolMajorVersion;
  USHORT ProtocolMinorVersion;
  USHORT ProtocolRevision;
  USHORT Reserved;
  DWORD Flags;
  struct {
    DWORD Reserved[8];
  } GenericReserved;
  struct {
    DWORD Reserved[16];
  } ProtocolSpecificReserved;
} FILE_REMOTE_PROTOCOL_INFO, *PFILE_REMOTE_PROTOCOL_INFO;

#endif /* _WIN32_WINNT < 0x0600 (prior to Windows Vista) */

typedef BOOL(WINAPI *MDBX_GetFileInformationByHandleEx)(_In_ HANDLE hFile,
                                                        _In_ FILE_INFO_BY_HANDLE_CLASS FileInformationClass,
                                                        _Out_ LPVOID lpFileInformation, _In_ DWORD dwBufferSize);

typedef BOOL(WINAPI *MDBX_GetVolumeInformationByHandleW)(
    _In_ HANDLE hFile, _Out_opt_ LPWSTR lpVolumeNameBuffer, _In_ DWORD nVolumeNameSize,
    _Out_opt_ LPDWORD lpVolumeSerialNumber, _Out_opt_ LPDWORD lpMaximumComponentLength,
    _Out_opt_ LPDWORD lpFileSystemFlags, _Out_opt_ LPWSTR lpFileSystemNameBuffer, _In_ DWORD nFileSystemNameSize);

typedef DWORD(WINAPI *MDBX_GetFinalPathNameByHandleW)(_In_ HANDLE hFile, _Out_ LPWSTR lpszFilePath,
                                                      _In_ DWORD cchFilePath, _In_ DWORD dwFlags);

typedef BOOL(WINAPI *MDBX_SetFileInformationByHandle)(_In_ HANDLE hFile,
                                                      _In_ FILE_INFO_BY_HANDLE_CLASS FileInformationClass,
                                                      _Out_ LPVOID lpFileInformation, _In_ DWORD dwBufferSize);

typedef NTSTATUS(NTAPI *MDBX_NtFsControlFile)(IN HANDLE FileHandle, IN OUT HANDLE Event,
                                              IN OUT PVOID /* PIO_APC_ROUTINE */ ApcRoutine, IN OUT PVOID ApcContext,
                                              OUT PIO_STATUS_BLOCK IoStatusBlock, IN ULONG FsControlCode,
                                              IN OUT PVOID InputBuffer, IN ULONG InputBufferLength,
                                              OUT OPTIONAL PVOID OutputBuffer, IN ULONG OutputBufferLength);

typedef uint64_t(WINAPI *MDBX_GetTickCount64)(void);

#if !defined(_WIN32_WINNT_WIN8) || _WIN32_WINNT < _WIN32_WINNT_WIN8
typedef struct _WIN32_MEMORY_RANGE_ENTRY {
  PVOID VirtualAddress;
  SIZE_T NumberOfBytes;
} WIN32_MEMORY_RANGE_ENTRY, *PWIN32_MEMORY_RANGE_ENTRY;
#endif /* Windows 8.x */

typedef BOOL(WINAPI *MDBX_PrefetchVirtualMemory)(HANDLE hProcess, ULONG_PTR NumberOfEntries,
                                                 PWIN32_MEMORY_RANGE_ENTRY VirtualAddresses, ULONG Flags);

typedef enum _SECTION_INHERIT { ViewShare = 1, ViewUnmap = 2 } SECTION_INHERIT;

typedef NTSTATUS(NTAPI *MDBX_NtExtendSection)(IN HANDLE SectionHandle, IN PLARGE_INTEGER NewSectionSize);

typedef LSTATUS(WINAPI *MDBX_RegGetValueA)(HKEY hkey, LPCSTR lpSubKey, LPCSTR lpValue, DWORD dwFlags, LPDWORD pdwType,
                                           PVOID pvData, LPDWORD pcbData);

typedef long(WINAPI *MDBX_CoCreateGuid)(bin128_t *guid);

NTSYSAPI ULONG RtlRandomEx(PULONG Seed);

typedef BOOL(WINAPI *MDBX_SetFileIoOverlappedRange)(HANDLE FileHandle, PUCHAR OverlappedRangeStart, ULONG Length);

struct libmdbx_imports {
  osal_srwlock_t_function srwl_Init;
  osal_srwlock_t_function srwl_AcquireShared;
  osal_srwlock_t_function srwl_ReleaseShared;
  osal_srwlock_t_function srwl_AcquireExclusive;
  osal_srwlock_t_function srwl_ReleaseExclusive;
  MDBX_NtExtendSection NtExtendSection;
  MDBX_GetFileInformationByHandleEx GetFileInformationByHandleEx;
  MDBX_GetVolumeInformationByHandleW GetVolumeInformationByHandleW;
  MDBX_GetFinalPathNameByHandleW GetFinalPathNameByHandleW;
  MDBX_SetFileInformationByHandle SetFileInformationByHandle;
  MDBX_NtFsControlFile NtFsControlFile;
  MDBX_PrefetchVirtualMemory PrefetchVirtualMemory;
  MDBX_GetTickCount64 GetTickCount64;
  MDBX_RegGetValueA RegGetValueA;
  MDBX_SetFileIoOverlappedRange SetFileIoOverlappedRange;
  MDBX_CoCreateGuid CoCreateGuid;
};

MDBX_INTERNAL void windows_import(void);
