# Creates a Python virtual environment in the build directory.
#
# Looks for a requirements.txt file in the same directory as the including CMake
# file and copies it into the build directory as an output file
#
# Creates the following variables: VIRTUALENV_DIRECTORY - base directory of
# Python venv VIRTUALENV_EXE_DIRECTORY - executable directory of installed venv
# tools
#
# Example: add_custom_target( Venv DEPENDS requirements.txt SOURCES
# "${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt" COMMENT "Python Venv tools")
#
# target_compile_definitions(unittests PUBLIC
# MYPY_PATH="${VIRTUALENV_EXE_DIRECTORY}/mypy${CMAKE_EXECUTABLE_SUFFIX}")
# add_dependencies(unittests Venv)
#
# TODO(ekilmer): This could be useful as something a bit more robust for other
# projects. Make at least a function

set(VIRTUALENV_DIRECTORY
    "${CMAKE_BINARY_DIR}/python/virtualenv"
    CACHE FILEPATH "Path to virtualenv")

if(WIN32)
  set(exe_dir "Scripts")
else()
  set(exe_dir "bin")
endif()

set(VIRTUALENV_EXE_DIRECTORY
    "${VIRTUALENV_DIRECTORY}/${exe_dir}"
    CACHE FILEPATH "Path to virtualenv directory containing executables")

add_custom_command(
  OUTPUT "${VIRTUALENV_DIRECTORY}"
  COMMAND "${Python3_EXECUTABLE}" -m venv "${VIRTUALENV_DIRECTORY}"
  COMMENT "Creating Python virtualenvironment at ${VIRTUALENV_DIRECTORY}")

add_custom_command(
  OUTPUT requirements.txt
  DEPENDS "${VIRTUALENV_DIRECTORY}"
          "${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt"
  COMMAND ${CMAKE_COMMAND} -E copy
          "${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt" requirements.txt
  COMMAND "${VIRTUALENV_EXE_DIRECTORY}/pip${CMAKE_EXECUTABLE_SUFFIX}" install
          --upgrade --requirement requirements.txt
  COMMENT "Installing Python packages from requirements.txt")
