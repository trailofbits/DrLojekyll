# Copyright 2021, Trail of Bits, Inc. All rights reserved.

function(compile_datalog)
  set(one_val_args LIBRARY_NAME SERVICE_NAME DATABASE_NAME CXX_OUTPUT_DIR
                   PY_OUTPUT_DIR DOT_OUTPUT_FILE DR_OUTPUT_FILE IR_OUTPUT_FILE
                   DRLOJEKYLL_CC DRLOJEKYLL_RT FB_OUTPUT_FILE WORKING_DIRECTORY
                   FIRST_ID)
  set(multi_val_args SOURCES DEPENDS INCLUDE_DIRECTORIES MODULE_DIRECTORIES LIBRARIES)
  cmake_parse_arguments(DR "" "${one_val_args}" "${multi_val_args}" ${ARGN})
  
  # Allow the caller to change the path of the Dr. Lojekyll compiler that
  # we will use.
  if(TARGET drlojekyll)
    message(STATUS "DrLojekyll: Using internal drlojekyll compiler")
    set(DR_DRLOJEKYLL_CC drlojekyll)

  else()
    find_package(DrLojekyll CONFIG QUIET)
    if(TARGET DrLojekyll::drlojekyll)
      set(DR_DRLOJEKYLL_CC DrLojekyll::drlojekyll)
      message(STATUS "DrLojekyll: Using local compiler from DrLojekyll installation")

    else()
      find_program(DR_DRLOJEKYLL_CC drlojekyll REQUIRED)
      if(NOT DR_DRLOJEKYLL_CC)
        message(FATAL_ERROR "DrLojekyll: No valid drlojekyll compiler found")
      endif()

      message(STATUS "DrLojekyll: Using drlojekyll compiler found from the PATH env variable")
    endif()
  endif()
  
  # Allow the caller to change the path of the Dr. Lojekyll runtime that
  # we will use.
  if(TARGET Runtime)
    set(DR_DRLOJEKYLL_RT DrLojekyll::Runtime)

  else()
    find_package(DrLojekyll CONFIG REQUIRED)
    set(DR_DRLOJEKYLL_RT DrLojekyll::Runtime)
  endif()

  set(dr_args "${DR_DRLOJEKYLL_CC}")
  
  # Database name. This influences file names. Otherwise the database name
  # is `datalog`. Database names can also be defined in the Datalog code itself.
  #
  # If the Datalog source code uses `#database foo.` then you should use
  # `DATABASE_NAME foo` to `compile_datalog`. Similarly, if the Datalog code
  # uses `#database "foo.bar".`, then you should use `DATABASE_NAME bar` to
  # `compile_datalog`.
  if(NOT DR_DATABASE_NAME)
    set(DR_DATABASE_NAME "datalog")
  endif()
  
  if(DR_MODULE_DIRECTORIES)
    foreach(module_dir ${DR_MODULE_DIRECTORIES})
      list(APPEND dr_args -M "${module_dir}")
    endforeach(module_dir)
  endif()
  
  if(NOT DR_WORKING_DIRECTORY)
    set(DR_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()

  # Output directory in which C++ code is placed.
  if(DR_CXX_OUTPUT_DIR)
    list(APPEND dr_args -cpp-out "${DR_CXX_OUTPUT_DIR}")

    set(dr_cxx_output_files
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.server.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.client.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.client.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.interface.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.db.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}_generated.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.cc")
  else()
    set(dr_cxx_output_files "")
  endif()

  # Output directory in which Python code is placed.
  if(DR_PY_OUTPUT_DIR)
    list(APPEND dr_args -py-out "${DR_PY_OUTPUT_DIR}")
    
    set(dr_py_output_files
      "${DR_PY_OUTPUT_DIR}/${DR_DATABASE_NAME}/__init__.py"
      "${DR_PY_OUTPUT_DIR}/${DR_DATABASE_NAME}/${DR_DATABASE_NAME}_grpc_db.py"
      "${DR_PY_OUTPUT_DIR}/${DR_DATABASE_NAME}/InputMessage.py"
      "${DR_PY_OUTPUT_DIR}/${DR_DATABASE_NAME}/OutputMessage.py"
      "${DR_PY_OUTPUT_DIR}/${DR_DATABASE_NAME}/Client.py")

  else()
    set(dr_py_output_files "")
  endif()

  # Debug output of the FlatBuffers schema. 
  if(DR_FB_OUTPUT_FILE)
    list(APPEND dr_args -flat-out "${DR_FB_OUTPUT_FILE}")
    list(APPEND dr_other_outputs "${DR_FB_OUTPUT_FILE}")
  endif(DR_FB_OUTPUT_FILE)

  # Debug output of the parsed representation as a single Dr. Lojekyll
  # Datalog file.
  if(DR_DR_OUTPUT_FILE)
    list(APPEND dr_args -dr-out "${DR_DR_OUTPUT_FILE}")
    list(APPEND dr_other_outputs "${DR_DR_OUTPUT_FILE}")
  endif(DR_DR_OUTPUT_FILE)

  # Debug output of the data flow intermediate representation as a
  # GraphViz DOT digraph.
  if(DR_DOT_OUTPUT_FILE)
    list(APPEND dr_args -dot-out "${DR_DOT_OUTPUT_FILE}")
    list(APPEND dr_other_outputs "${DR_DOT_OUTPUT_FILE}")
  endif(DR_DOT_OUTPUT_FILE)

  # Debug output of the control-flow intermediate representation.
  if(DR_IR_OUTPUT_FILE)
    list(APPEND dr_args -ir-out "${DR_IR_OUTPUT_FILE}")
    list(APPEND dr_other_outputs "${DR_IR_OUTPUT_FILE}")
  endif(DR_IR_OUTPUT_FILE)
  
  # For changing the codegen to start with a different ID than 0. Helps when
  # multiple auto-generated headers are included in the same spot.
  if(DR_FIRST_ID)
    list(APPEND dr_args -first-id "${DR_FIRST_ID}")
  endif(DR_FIRST_ID)

  # Datalog source files to compile.
  if(NOT DR_SOURCES)
    message(FATAL_ERROR "compile_datalog function requires at least one SOURCES parameter")
  endif()
  list(APPEND dr_args ${DR_SOURCES})
  
  foreach(source_file ${DR_SOURCES})
    if(EXISTS "${DR_WORKING_DIRECTORY}/${source_file}")
      list(APPEND absolute_sources "${DR_WORKING_DIRECTORY}/${source_file}")
    else()
      list(APPEND absolute_sources "${source_file}")
    endif()
  endforeach(source_file)
  
  if(DR_LIBRARY_NAME OR DR_SERVICE_NAME)
    find_package(gRPC CONFIG REQUIRED)
    find_package(Flatbuffers CONFIG REQUIRED)
  endif()

  add_custom_command(
    OUTPUT
      ${dr_cxx_output_files}
      ${dr_py_output_files}
      ${dr_other_outputs}
    COMMAND
      ${dr_args}
    COMMENT
      "Compiling ${DR_DATABASE_NAME} datalog code in directory ${DR_WORKING_DIRECTORY}"
    WORKING_DIRECTORY
      "${DR_WORKING_DIRECTORY}"
    DEPENDS
      "${DR_DRLOJEKYLL_CC}"
      ${absolute_sources}
      ${DR_DEPENDS})

  # Allow ourselves to use the same `DATABASE_NAME` in different locations.  
  string(MD5 target_hash "${DR_CXX_OUTPUT_DIR}/${DR_PY_OUTPUT_DIR}")
  set(target_base "${DR_DATABASE_NAME}_${target_hash}")

  add_custom_target("${target_base}_outputs" DEPENDS
    ${dr_cxx_output_files}
    ${dr_py_output_files}
    ${dr_other_outputs})
  
  set(runtime_libs
    ${DR_DRLOJEKYLL_RT}
    gRPC::gpr gRPC::upb gRPC::grpc gRPC::grpc++
    flatbuffers::flatbuffers
    ${DR_LIBRARIES}
  )
  
  if(TARGET DrLojekyll::drlojekyll_sanitizers)
    list(APPEND runtime_libs DrLojekyll::drlojekyll_sanitizers)
  elseif(TARGET drlojekyll_sanitizers)
    list(APPEND runtime_libs drlojekyll_sanitizers)
  endif()

  # Generate a library that we can use to link against the generated C++ code,
  # e.g. to make custom instances of the database.
  if(DR_LIBRARY_NAME)
    if(NOT DR_CXX_OUTPUT_DIR)
      message(FATAL_ERROR "CXX_OUTPUT_DIR argument to compile_datalog is required when using LIBRARY_NAME")
    endif()
      
    add_library("${DR_LIBRARY_NAME}" STATIC
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.db.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}_generated.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.client.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.client.h")
    
    add_dependencies("${DR_LIBRARY_NAME}"
      "${target_base}_outputs")

    target_link_libraries("${DR_LIBRARY_NAME}" PUBLIC
      ${runtime_libs})

    target_include_directories("${DR_LIBRARY_NAME}"
      PUBLIC
        $<BUILD_INTERFACE:${DR_CXX_OUTPUT_DIR}>
      PRIVATE
        $<BUILD_INTERFACE:${DR_WORKING_DIRECTORY}>
        ${DR_INCLUDE_DIRECTORIES})
  
  endif()
  
  # Generate an executable that we can set up as a standalone service that will
  # run the database as a server.
  if(DR_SERVICE_NAME)

    add_executable("${DR_SERVICE_NAME}"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.server.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.cc")

    add_dependencies("${DR_SERVICE_NAME}"
      "${target_base}_outputs")

    target_include_directories("${DR_SERVICE_NAME}"
      PRIVATE
        $<BUILD_INTERFACE:${DR_CXX_OUTPUT_DIR}>
        $<BUILD_INTERFACE:${DR_WORKING_DIRECTORY}>
        ${DR_INCLUDE_DIRECTORIES})

    target_link_libraries("${DR_SERVICE_NAME}" PRIVATE
      ${runtime_libs})
  endif()
  
endfunction(compile_datalog)
