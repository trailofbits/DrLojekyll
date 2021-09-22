# Copyright 2021, Trail of Bits, Inc. All rights reserved.

function(compile_datalog)
  set(one_val_args LIBRARY_NAME SERVICE_NAME DATABASE_NAME CXX_OUTPUT_DIR PY_OUTPUT_DIR
                   DOT_OUTPUT_FILE DR_OUTPUT_FILE IR_OUTPUT_FILE DRLOJEKYLL_CC
                   DRLOJEKYLL_RT)
  set(multi_val_args SOURCES)
  cmake_parse_arguments(DR "" "${one_val_args}" "${multi_val_args}" ${ARGN})
  
  # Allow the caller to change the path of the Dr. Lojekyll compiler that
  # we will use.
  if(NOT DR_DRLOJEKYLL_CC)
    find_package(DrLojekyll CONFIG REQUIRED)
    set(DR_DRLOJEKYLL_CC DrLojekyll::drlojekyll)
    if (DrLojekyll::drlojekyll-NOTFOUND)
      find_program(DR_DRLOJEKYLL_CC drlojekyll REQUIRED)
    endif()
  endif()
  
  # Allow the caller to change the path of the Dr. Lojekyll runtime that
  # we will use.
  if(NOT DR_DRLOJEKYLL_RT)
    find_package(DrLojekyll CONFIG REQUIRED)
    set(DR_DRLOJEKYLL_RT DrLojekyll::Runtime)
  endif()

  set(dr_args ${DR_DRLOJEKYLL_CC})
  
  # Database name. This influences file names. Otherwise the database name
  # is `datalog`. Database names can also be defined in the Datalog code itself.
  if(DR_DATABASE_NAME)
    list(APPEND dr_args -database "${DR_DATABASE_NAME}")
  else()
    message(FATAL_ERROR "DATABASE_NAME parameter of compile_datalog is required")
  endif()

  # Output directory in which C++ code is placed.
  set(dr_cxx_output_files)
  if(DR_CXX_OUTPUT_DIR)
    list(APPEND dr_args -cpp-out "${DR_CXX_OUTPUT_DIR}")
    
    set(dr_cxx_output_files
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.interface.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.db.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}_generated.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.cc")
  endif()

  # Output directory in which Python code is placed.
  if(DR_PY_OUTPUT_DIR)
    list(APPEND dr_args -py-out "${DR_PY_OUTPUT_DIR}")
  endif()

  # Debug output of the parsed representation as a single Dr. Lojekyll
  # Datalog file.
  if(DR_DR_OUTPUT_FILE)
    list(APPEND dr_args -dr-out "${DR_DR_OUTPUT_FILE}")
  endif()

  # Debug output of the data flow intermediate representation as a
  # GraphViz DOT digraph.
  if(DR_DOT_OUTPUT_FILE)
    list(APPEND dr_args -dot-out "${DR_DOT_OUTPUT_FILE}")
  endif()

  # Debug output of the control-flow intermediate representation.
  if(DR_IR_OUTPUT_FILE)
    list(APPEND dr_args -ir-out "${DR_IR_OUTPUT_FILE}")
  endif()
  
  # Datalog source files to compile.
  if(NOT DR_SOURCES)
    message(FATAL_ERROR "compile_datalog function requires at least one SOURCES parameter")
  endif()
  list(APPEND dr_args ${DR_SOURCES})
  
  add_custom_command(
    OUTPUT ${dr_cxx_output_files}
    COMMAND ${dr_args}
    COMMENT "Compiling Datalog code"
    DEPENDS ${DR_DRLOJEKYLL_CC}
            ${DR_SOURCES})
  
  # Generate a library that we can use to link against the generated C++ code,
  # e.g. to make custom instances of the database.
  if(DR_LIBRARY_NAME)
    if(NOT DR_CXX_OUTPUT_DIR)
      message(FATAL_ERROR "CXX_OUTPUT_DIR argument to compile_datalog is required when using LIBRARY_NAME")
    endif()
        
    add_custom_target(${DR_LIBRARY_NAME}-files DEPENDS
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.db.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}_generated.h")
    add_library(${DR_LIBRARY_NAME} INTERFACE)
    
    target_link_libraries(${DR_LIBRARY_NAME} INTERFACE
      ${DR_DRLOJEKYLL_RT}
      ${DR_LIBRARY_NAME}-files)
      
    target_include_directories(${DR_LIBRARY_NAME} INTERFACE "${DR_CXX_OUTPUT_DIR}")
  
  endif()
  
  # Generate an executable that we can set up as a standalone service that will
  # run the database as a server.
  if(DR_SERVICE_NAME)
  
    find_package(flatbuffers CONFIG REQUIRED)
    
    add_executable(${DR_SERVICE_NAME}
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.cc")
    
    target_include_directories(${DR_SERVICE_NAME} INTERFACE "${DR_CXX_OUTPUT_DIR}")
    
    target_link_libraries(${DR_SERVICE_NAME} PRIVATE
      ${DR_DRLOJEKYLL_RT}
      flatbuffers)
  endif()
  
endfunction(compile_datalog)