# Copyright 2020, Trail of Bits, Inc. All rights reserved.

include(ExternalProject)

set(OPENSSL_DOWNLOAD_URL "https://www.openssl.org/source/openssl-1.1.1k.tar.gz")
set(install_destination "${CMAKE_CURRENT_BINARY_DIR}/installed")

set(openssl_configure_options
  no-ssl2
  no-ssl3
  no-asm
  no-shared
  no-weak-ssl-ciphers
  no-comp
  enable-cms
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(libcrypto_path "${install_destination}/lib64/libcrypto.a")
  set(libssl_path "${install_destination}/lib64/libssl.a")

  ExternalProject_Add(thirdparty_openssl_builder
    DOWNLOAD_DIR
      "${CMAKE_CURRENT_BINARY_DIR}"

    URL
      "${OPENSSL_DOWNLOAD_URL}"

    CONFIGURE_COMMAND
      "${CMAKE_COMMAND}" -E env CC="${CMAKE_C_COMPILER}" AR="${CMAKE_AR}"
      perl ./Configure linux-x86_64 ${openssl_configure_options}
            "--openssldir=/etc/openssl" -fPIC ${sysroot_option}
            ${unwind_library} -pthread

    BUILD_COMMAND
      make depend &&
      make -j 2

    INSTALL_COMMAND
      make DESTDIR="${install_destination}" install_sw install_ssldirs

    BUILD_IN_SOURCE
      true

    BUILD_BYPRODUCTS
      "${libcrypto_path}"
      "${libssl_path}"
  )

elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  if(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(target_name
      "darwin64-arm64-cc"
    )
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    set(target_name
      "darwin64-x86_64-cc"
    )
  else()
    message(FATAL_ERROR "Unsupported target processor")
  endif()

  set(libcrypto_path "${install_destination}/lib/libcrypto.a")
  set(libssl_path "${install_destination}/lib/libssl.a")

  ExternalProject_Add(thirdparty_openssl_builder
    DOWNLOAD_DIR
      "${CMAKE_CURRENT_BINARY_DIR}"

    URL
      "${OPENSSL_DOWNLOAD_URL}"

    CONFIGURE_COMMAND
      "${CMAKE_COMMAND}" -E env CC="${CMAKE_C_COMPILER}" AR="${CMAKE_AR}"
      perl ./Configure "${target_name}" ${openssl_configure_options}
            "--openssldir=/etc/openssl" -fPIC

    BUILD_COMMAND
      /usr/bin/sed -i ".bak" "s+^CFLAGS=+CFLAGS=-isysroot ${CMAKE_OSX_SYSROOT} +g" "Makefile" &&
      "${CMAKE_COMMAND}" -E make_directory "${install_destination}/etc/openssl" &&
      make depend &&
      make -j 2

    INSTALL_COMMAND
      make DESTDIR="${install_destination}" install_sw install_ssldirs

    BUILD_IN_SOURCE
      true

    BUILD_BYPRODUCTS
      "${libcrypto_path}"
      "${libssl_path}"
  )

elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  find_package(Perl REQUIRED)
  string(REPLACE "/" "\\\\" windows_formatted_perl_path "${PERL_EXECUTABLE}")

  set(libcrypto_path "${install_destination}/lib/libcrypto.lib")
  set(libssl_path "${install_destination}/lib/libssl.lib")

  if(CMAKE_SIZEOF_VOID_P EQUAL 4)
    set(architecture VC-WIN32)
  else()
    set(architecture VC-WIN64A)
  endif()

  ExternalProject_Add(thirdparty_openssl_builder
    DOWNLOAD_DIR
      "${CMAKE_CURRENT_BINARY_DIR}"

    URL
      "${OPENSSL_DOWNLOAD_URL}"

    CONFIGURE_COMMAND
      "${CMAKE_COMMAND}" -E env "${PERL_EXECUTABLE}" Configure ${architecture} ${openssl_configure_options}

    BUILD_COMMAND
    "${CMAKE_COMMAND}" -E env "cmd.exe" "/C" "nmake PERL=${windows_formatted_perl_path} build_libs"

    INSTALL_COMMAND
      "${CMAKE_COMMAND}" -E make_directory "${install_destination}/lib" &&
      "${CMAKE_COMMAND}" -E copy "./libssl.lib" "${install_destination}/lib" &&
      "${CMAKE_COMMAND}" -E copy "./libcrypto.lib" "${install_destination}/lib" &&
      "${CMAKE_COMMAND}" -E make_directory "${install_destination}/include" &&
      "${CMAKE_COMMAND}" -E copy_directory "./include/openssl" "${install_destination}/include/openssl"

    BUILD_IN_SOURCE
      true

    BUILD_BYPRODUCTS
      "${libcrypto_path}"
      "${libssl_path}"
  )

else()
  message(FATAL_ERROR "Unsupported system")
endif()

# Import the crypto library
add_library(thirdparty_openssl_crypto_private STATIC IMPORTED GLOBAL)
add_dependencies(thirdparty_openssl_crypto_private
  thirdparty_openssl_builder
)

set_target_properties(thirdparty_openssl_crypto_private PROPERTIES
  IMPORTED_LOCATION
    "${libcrypto_path}"
)

if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  target_link_libraries(thirdparty_openssl_crypto_private INTERFACE
    ws2_32.lib
    mswsock.lib
    crypt32.lib
  )
endif()

# Import the SSL library
add_library(thirdparty_openssl_ssl_private STATIC IMPORTED GLOBAL)
add_dependencies(thirdparty_openssl_ssl_private
  thirdparty_openssl_builder
)

set_target_properties(thirdparty_openssl_ssl_private PROPERTIES
  IMPORTED_LOCATION
    "${libssl_path}"
)

# Create the interface targets, so we have a place where we can
# attach the include directories even if they do not exist on disk
add_library(thirdparty_openssl_ssl INTERFACE)
add_library(OpenSSL::SSL ALIAS thirdparty_openssl_ssl)

target_link_libraries(thirdparty_openssl_ssl INTERFACE
  thirdparty_openssl_ssl_private
)

add_library(thirdparty_openssl_crypto INTERFACE)
add_library(OpenSSL::Crypto ALIAS thirdparty_openssl_crypto)

target_link_libraries(thirdparty_openssl_crypto INTERFACE
  thirdparty_openssl_crypto_private
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_package(Threads REQUIRED)
endif()

foreach(library_name thirdparty_openssl_ssl thirdparty_openssl_crypto)
  target_include_directories("${library_name}" INTERFACE
    "${install_destination}/usr/local/include"
  )
  
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries("${library_name}" INTERFACE
      Threads::Threads
      ${CMAKE_DL_LIBS}
    )
  endif()
endforeach()
