##  Copyright (c) 2012-2024 Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru>
##  SPDX-License-Identifier: Apache-2.0

if(CMAKE_VERSION VERSION_LESS 3.8.2)
  cmake_minimum_required(VERSION 3.0.2)
elseif(CMAKE_VERSION VERSION_LESS 3.12)
  cmake_minimum_required(VERSION 3.8.2)
else()
  cmake_minimum_required(VERSION 3.12)
endif()

cmake_policy(PUSH)
cmake_policy(VERSION ${CMAKE_MINIMUM_REQUIRED_VERSION})

macro(add_compile_flags languages)
  foreach(_lang ${languages})
    string(REPLACE ";" " " _flags "${ARGN}")
    if(CMAKE_CXX_COMPILER_LOADED AND _lang STREQUAL "CXX")
      set("${_lang}_FLAGS" "${${_lang}_FLAGS} ${_flags}")
    endif()
    if(CMAKE_C_COMPILER_LOADED AND _lang STREQUAL "C")
      set("${_lang}_FLAGS" "${${_lang}_FLAGS} ${_flags}")
    endif()
  endforeach()
  unset(_lang)
  unset(_flags)
endmacro(add_compile_flags)

macro(remove_flag varname flag)
  string(REGEX REPLACE "^(.*)( ${flag} )(.*)$" "\\1 \\3" ${varname} ${${varname}})
  string(REGEX REPLACE "^((.+ )*)(${flag})(( .+)*)$" "\\1\\4" ${varname} ${${varname}})
endmacro(remove_flag)

macro(remove_compile_flag languages flag)
  foreach(_lang ${languages})
    if(CMAKE_CXX_COMPILER_LOADED AND _lang STREQUAL "CXX")
      remove_flag(${_lang}_FLAGS ${flag})
    endif()
    if(CMAKE_C_COMPILER_LOADED AND _lang STREQUAL "C")
      remove_flag(${_lang}_FLAGS ${flag})
    endif()
  endforeach()
  unset(_lang)
endmacro(remove_compile_flag)

macro(set_source_files_compile_flags)
  foreach(file ${ARGN})
    get_filename_component(_file_ext ${file} EXT)
    set(_lang "")
    if("${_file_ext}" STREQUAL ".m")
      set(_lang OBJC)
      # CMake believes that Objective C is a flavor of C++, not C,
      # and uses g++ compiler for .m files.
      # LANGUAGE property forces CMake to use CC for ${file}
      set_source_files_properties(${file} PROPERTIES LANGUAGE C)
    elseif("${_file_ext}" STREQUAL ".mm")
      set(_lang OBJCXX)
    endif()

    if(_lang)
      get_source_file_property(_flags ${file} COMPILE_FLAGS)
      if("${_flags}" STREQUAL "NOTFOUND")
        set(_flags "${CMAKE_${_lang}_FLAGS}")
      else()
        set(_flags "${_flags} ${CMAKE_${_lang}_FLAGS}")
      endif()
      # message(STATUS "Set (${file} ${_flags}")
      set_source_files_properties(${file} PROPERTIES COMPILE_FLAGS
        "${_flags}")
    endif()
  endforeach()
  unset(_file_ext)
  unset(_lang)
endmacro(set_source_files_compile_flags)

macro(fetch_version name source_root_directory parent_scope build_directory_for_json_output)
  set(_version_4dot "")
  set(_git_describe "")
  set(_git_timestamp "")
  set(_git_tree "")
  set(_git_commit "")
  set(_git_revision 0)
  set(_git_version "")
  set(_version_from "")
  set(_git_root FALSE)

  find_program(GIT git)
  if(GIT)
    execute_process(COMMAND ${GIT} rev-parse --show-toplevel
      OUTPUT_VARIABLE _git_root
      ERROR_VARIABLE _git_root_error
      OUTPUT_STRIP_TRAILING_WHITESPACE
      WORKING_DIRECTORY ${source_root_directory}
      RESULT_VARIABLE _rc)
    if(_rc OR _git_root STREQUAL "")
      if(EXISTS "${source_root_directory}/.git")
        message(ERROR "`git rev-parse --show-toplevel` failed '${_git_root_error}'")
      else()
        message(VERBOSE "`git rev-parse --show-toplevel` failed '${_git_root_error}'")
      endif()
    else()
      set(_source_root "${source_root_directory}")
      if(NOT CMAKE_VERSION VERSION_LESS 3.20)
        cmake_path(NORMAL_PATH _git_root)
        cmake_path(NORMAL_PATH _source_root)
      endif()
      if(_source_root STREQUAL _git_root AND EXISTS "${_git_root}/VERSION.json")
        message(FATAL_ERROR "Несколько источников информации о версии, допустим только один из: репозиторий git, либо файл VERSION.json")
      endif()
    endif()
  endif()

  if(EXISTS "${source_root_directory}/VERSION.json")
    set(_version_from "${source_root_directory}/VERSION.json")

    if(CMAKE_VERSION VERSION_LESS 3.19)
      message(FATAL_ERROR "Требуется CMake версии >= 3.19 для чтения VERSION.json")
    endif()
    file(STRINGS "${_version_from}" _versioninfo_json NEWLINE_CONSUME LIMIT_COUNT 9 LIMIT_INPUT 999 ENCODING UTF-8)
    string(JSON _git_describe GET ${_versioninfo_json} git_describe)
    string(JSON _git_timestamp GET "${_versioninfo_json}" "git_timestamp")
    string(JSON _git_tree GET "${_versioninfo_json}" "git_tree")
    string(JSON _git_commit GET "${_versioninfo_json}" "git_commit")
    string(JSON _version_4dot GET "${_versioninfo_json}" "version_4dot")
    unset(_json_object)
    string(REPLACE "." ";" _version_list "${_version_4dot}")

    if(NOT _version_4dot)
      message(ERROR "Unable to retrieve ${name} version from \"${_version_from}\" file.")
      set(_version_list ${_git_version})
      string(REPLACE ";" "." _version_4dot "${_git_version}")
    else()
      string(REPLACE "." ";" _version_list ${_version_4dot})
    endif()

  elseif(_git_root AND _source_root STREQUAL _git_root)
    set(_version_from git)

    execute_process(COMMAND ${GIT} show --no-patch --format=%cI HEAD
      OUTPUT_VARIABLE _git_timestamp
      OUTPUT_STRIP_TRAILING_WHITESPACE
      WORKING_DIRECTORY ${source_root_directory}
      RESULT_VARIABLE _rc)
    if(_rc OR _git_timestamp STREQUAL "%cI")
      execute_process(COMMAND ${GIT} show --no-patch --format=%ci HEAD
        OUTPUT_VARIABLE _git_timestamp
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${source_root_directory}
        RESULT_VARIABLE _rc)
      if(_rc OR _git_timestamp STREQUAL "%ci")
        message(FATAL_ERROR "Please install latest version of git (`show --no-patch --format=%cI HEAD` failed)")
      endif()
    endif()

    execute_process(COMMAND ${GIT} show --no-patch --format=%T HEAD
      OUTPUT_VARIABLE _git_tree
      OUTPUT_STRIP_TRAILING_WHITESPACE
      WORKING_DIRECTORY ${source_root_directory}
      RESULT_VARIABLE _rc)
    if(_rc OR _git_tree STREQUAL "")
      message(FATAL_ERROR "Please install latest version of git (`show --no-patch --format=%T HEAD` failed)")
    endif()

    execute_process(COMMAND ${GIT} show --no-patch --format=%H HEAD
      OUTPUT_VARIABLE _git_commit
      OUTPUT_STRIP_TRAILING_WHITESPACE
      WORKING_DIRECTORY ${source_root_directory}
      RESULT_VARIABLE _rc)
    if(_rc OR _git_commit STREQUAL "")
      message(FATAL_ERROR "Please install latest version of git (`show --no-patch --format=%H HEAD` failed)")
    endif()

    execute_process(COMMAND ${GIT} status --untracked-files=no --porcelain
      OUTPUT_VARIABLE _git_status
      OUTPUT_STRIP_TRAILING_WHITESPACE
      WORKING_DIRECTORY ${source_root_directory}
      RESULT_VARIABLE _rc)
    if(_rc)
      message(FATAL_ERROR "Please install latest version of git (`status --untracked-files=no --porcelain` failed)")
    endif()
    if(NOT _git_status STREQUAL "")
      set(_git_commit "${_git_commit}-dirty")
    endif()
    unset(_git_status)

    execute_process(COMMAND ${GIT} rev-list --tags --count
      OUTPUT_VARIABLE _tag_count
      OUTPUT_STRIP_TRAILING_WHITESPACE
      WORKING_DIRECTORY ${source_root_directory}
      RESULT_VARIABLE _rc)
    if(_rc)
      message(FATAL_ERROR "Please install latest version of git (`git rev-list --tags --count` failed)")
    endif()

    if(_tag_count EQUAL 0)
      execute_process(COMMAND ${GIT} rev-list --all --count
        OUTPUT_VARIABLE _whole_count
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${source_root_directory}
        RESULT_VARIABLE _rc)
      if(_rc)
        message(FATAL_ERROR "Please install latest version of git (`git rev-list --all --count` failed)")
      endif()
      if(_whole_count GREATER 42)
        message(FATAL_ERROR "Please fetch tags (no any tags for ${_whole_count} commits)")
      endif()
      set(_git_version "0;0;0")
      execute_process(COMMAND ${GIT} rev-list --count --all --no-merges
        OUTPUT_VARIABLE _git_revision
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${source_root_directory}
        RESULT_VARIABLE _rc)
      if(_rc OR _git_revision STREQUAL "")
        message(FATAL_ERROR "Please install latest version of git (`rev-list --count --all --no-merges` failed)")
      endif()
    else(_tag_count EQUAL 0)
      execute_process(COMMAND ${GIT} describe --tags --long --dirty=-dirty "--match=v[0-9]*"
        OUTPUT_VARIABLE _git_describe
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${source_root_directory}
        RESULT_VARIABLE _rc)
      if(_rc OR _git_describe STREQUAL "")
        execute_process(COMMAND ${GIT} rev-list --all --count
          OUTPUT_VARIABLE _whole_count
          OUTPUT_STRIP_TRAILING_WHITESPACE
          WORKING_DIRECTORY ${source_root_directory}
         RESULT_VARIABLE _rc)
         if(_rc)
          message(FATAL_ERROR "Please install latest version of git (`git rev-list --all --count` failed)")
        endif()
        if(_whole_count GREATER 42)
          message(FATAL_ERROR "Please fetch tags (`describe --tags --long --dirty --match=v[0-9]*` failed)")
        else()
          execute_process(COMMAND ${GIT} describe --all --long --dirty=-dirty
            OUTPUT_VARIABLE _git_describe
            OUTPUT_STRIP_TRAILING_WHITESPACE
            WORKING_DIRECTORY ${source_root_directory}
            RESULT_VARIABLE _rc)
          if(_rc OR _git_describe STREQUAL "")
            message(FATAL_ERROR "Please install latest version of git (`git rev-list --tags --count` and/or `git rev-list --all --count` failed)")
          endif()
        endif()
      endif()

      execute_process(COMMAND ${GIT} describe --tags --abbrev=0 "--match=v[0-9]*"
        OUTPUT_VARIABLE _last_release_tag
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${source_root_directory}
        RESULT_VARIABLE _rc)
      if(_rc)
        message(FATAL_ERROR "Please install latest version of git (`describe --tags --abbrev=0 --match=v[0-9]*` failed)")
      endif()
      if (_last_release_tag)
        set(_git_revlist_arg "${_last_release_tag}..HEAD")
      else()
        execute_process(COMMAND ${GIT} tag --sort=-version:refname
          OUTPUT_VARIABLE _tag_list
          OUTPUT_STRIP_TRAILING_WHITESPACE
          WORKING_DIRECTORY ${source_root_directory}
          RESULT_VARIABLE _rc)
        if(_rc)
          message(FATAL_ERROR "Please install latest version of git (`tag --sort=-version:refname` failed)")
        endif()
        string(REGEX REPLACE "\n" ";" _tag_list "${_tag_list}")
        set(_git_revlist_arg "HEAD")
        foreach(_tag IN LISTS _tag_list)
          if(NOT _last_release_tag)
            string(REGEX MATCH "^v[0-9]+(\.[0-9]+)+" _last_release_tag "${_tag}")
            set(_git_revlist_arg "${_tag}..HEAD")
          endif()
        endforeach(_tag)
      endif()
      execute_process(COMMAND ${GIT} rev-list --count "${_git_revlist_arg}"
        OUTPUT_VARIABLE _git_revision
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${source_root_directory}
        RESULT_VARIABLE _rc)
      if(_rc OR _git_revision STREQUAL "")
        message(FATAL_ERROR "Please install latest version of git (`rev-list --count ${_git_revlist_arg}` failed)")
      endif()

      string(REGEX MATCH "^(v)?([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)?" _git_version_valid "${_git_describe}")
      if(_git_version_valid)
        string(REGEX REPLACE "^(v)?([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)?" "\\2;\\3;\\4" _git_version ${_git_describe})
      else()
        string(REGEX MATCH "^(v)?([0-9]+)\\.([0-9]+)(.*)?" _git_version_valid "${_git_describe}")
        if(_git_version_valid)
          string(REGEX REPLACE "^(v)?([0-9]+)\\.([0-9]+)(.*)?" "\\2;\\3;0" _git_version ${_git_describe})
        else()
          message(AUTHOR_WARNING "Bad ${name} version \"${_git_describe}\"; falling back to 0.0.0 (have you made an initial release?)")
          set(_git_version "0;0;0")
        endif()
      endif()
    endif(_tag_count EQUAL 0)

    list(APPEND _git_version "${_git_revision}")
    set(_version_list "${_git_version}")
    string(REPLACE ";" "." _version_4dot "${_version_list}")

  elseif(GIT)
    message(FATAL_ERROR "Нет источника информации о версии (${source_root_directory}), требуется один из: репозиторий git, либо VERSION.json")
  else()
    message(FATAL_ERROR "Требуется git для получения информации о версии")
  endif()

  list(LENGTH _version_list _version_list_length)
  list(GET _version_list 0 _version_major)
  list(GET _version_list 1 _version_minor)
  list(GET _version_list 2 _version_release)
  list(GET _version_list 3 _version_revision)

  if(NOT _git_describe OR NOT _git_timestamp OR NOT _git_tree OR NOT _git_commit OR _git_revision STREQUAL "" OR NOT _version_list_length EQUAL 4 OR _version_major STREQUAL "" OR _version_minor STREQUAL "" OR _version_release STREQUAL "" OR _version_revision STREQUAL "")
    message(ERROR "Unable to retrieve ${name} version from ${_version_from}.")
  else()
    list(APPEND _git_version "${_git_revision}")
  endif()

  if(${parent_scope})
    set(${name}_VERSION_MAJOR "${_version_major}" PARENT_SCOPE)
    set(${name}_VERSION_MINOR "${_version_minor}" PARENT_SCOPE)
    set(${name}_VERSION_RELEASE "${_version_release}" PARENT_SCOPE)
    set(${name}_VERSION_REVISION "${_version_revision}" PARENT_SCOPE)
    set(${name}_VERSION "${_version_4dot}" PARENT_SCOPE)

    set(${name}_GIT_DESCRIBE "${_git_describe}" PARENT_SCOPE)
    set(${name}_GIT_TIMESTAMP "${_git_timestamp}" PARENT_SCOPE)
    set(${name}_GIT_TREE "${_git_tree}" PARENT_SCOPE)
    set(${name}_GIT_COMMIT "${_git_commit}" PARENT_SCOPE)
    set(${name}_GIT_REVISION "${_git_revision}" PARENT_SCOPE)
  else()
    set(${name}_VERSION_MAJOR "${_version_major}")
    set(${name}_VERSION_MINOR "${_version_minor}")
    set(${name}_VERSION_RELEASE "${_version_release}")
    set(${name}_VERSION_REVISION "${_version_revision}")
    set(${name}_VERSION "${_version_4dot}")

    set(${name}_GIT_DESCRIBE "${_git_describe}")
    set(${name}_GIT_TIMESTAMP "${_git_timestamp}")
    set(${name}_GIT_TREE "${_git_tree}")
    set(${name}_GIT_COMMIT "${_git_commit}")
    set(${name}_GIT_REVISION "${_git_revision}")
  endif()

  if(_version_from STREQUAL "git")
    string(CONFIGURE "{
      \"git_describe\" : \"@_git_describe@\",
      \"git_timestamp\" : \"@_git_timestamp@\",
      \"git_tree\" : \"@_git_tree@\",
      \"git_commit\" : \"@_git_commit@\",
      \"version_4dot\" : \"@_version_4dot@\"\n}" _versioninfo_json @ONLY ESCAPE_QUOTES)
    file(WRITE "${build_directory_for_json_output}/VERSION.json" "${_versioninfo_json}")
  endif()
endmacro(fetch_version)

cmake_policy(POP)
