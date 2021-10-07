# Copyright 2021, Trail of Bits, Inc. All rights reserved.

function(compile_datalog)
  set(one_val_args LIBRARY_NAME SERVICE_NAME DATABASE_NAME CXX_OUTPUT_DIR
                   PY_OUTPUT_DIR DOT_OUTPUT_FILE DR_OUTPUT_FILE IR_OUTPUT_FILE
                   DRLOJEKYLL_CC DRLOJEKYLL_RT FB_OUTPUT_FILE WORKING_DIRECTORY)
  set(multi_val_args SOURCES LIBRARIES)
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
  if(DR_DATABASE_NAME)
    list(APPEND dr_args -database "${DR_DATABASE_NAME}")
  else()
    message(FATAL_ERROR "DATABASE_NAME parameter of compile_datalog is required")
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
    OUTPUT ${dr_cxx_output_files} ${dr_py_output_files}
    COMMAND ${dr_args}
    COMMENT "Compiling ${DATABASE_NAME} datalog code"
    WORKING_DIRECTORY "${DR_WORKING_DIRECTORY}"
    DEPENDS ${DR_DRLOJEKYLL_CC}
            ${DR_SOURCES})
  
  set(runtime_libs
    ${DR_DRLOJEKYLL_RT}
    gRPC::gpr gRPC::upb gRPC::grpc gRPC::grpc++
    flatbuffers::flatbuffers
  )
  
  if(DR_LIBRARY_NAME OR DR_SERVICE_NAME)
    find_package(gRPC CONFIG REQUIRED)
    find_package(Flatbuffers CONFIG REQUIRED)
  endif()
  
  # Generate a library that we can use to link against the generated C++ code,
  # e.g. to make custom instances of the database.
  if(DR_LIBRARY_NAME)
    if(NOT DR_CXX_OUTPUT_DIR)
      message(FATAL_ERROR "CXX_OUTPUT_DIR argument to compile_datalog is required when using LIBRARY_NAME")
    endif()
      
    add_library(${DR_LIBRARY_NAME} STATIC
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.db.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}_generated.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.client.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.client.h")

    target_link_libraries(${DR_LIBRARY_NAME} PUBLIC
      ${runtime_libs})

    target_include_directories(${DR_LIBRARY_NAME} PUBLIC
      "${DR_CXX_OUTPUT_DIR}")
  
  endif()
  
  # Generate an executable that we can set up as a standalone service that will
  # run the database as a server.
  if(DR_SERVICE_NAME)
    
    add_executable(${DR_SERVICE_NAME}
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.server.cpp"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.h"
      "${DR_CXX_OUTPUT_DIR}/${DR_DATABASE_NAME}.grpc.fb.cc")
    
    target_include_directories(${DR_SERVICE_NAME} INTERFACE "${DR_CXX_OUTPUT_DIR}")
    
    target_link_libraries(${DR_SERVICE_NAME} PRIVATE
      ${runtime_libs})
  endif()
  
endfunction(compile_datalog)
