# Copyright 2020, Trail of Bits, Inc. All rights reserved.

# Add to or create an interface library that enables a requested sanitizer.
# Sanitizers are requested using the `SANITIZERS` keyword.
# Additional compile and link options can be specified using the `OPTIONS` keyword.
#
# Example usage:
#
#     add_sanitizer_settings(settings
#         SANITIZERS
#             address
#             undefined
#         OPTIONS
#             -fsanitize-address-use-after-scope
#     )
#
# FIXME: This will only possibly work at present with compilers that understand GNU-style options
function(add_sanitizer_settings target_name)
    cmake_parse_arguments(PARSE_ARGV 1 SAN
        ""
        ""
        "SANITIZERS;OPTIONS")

    string(REPLACE ";" "," SANITIZER_LIST "${SAN_SANITIZERS}")

    if(NOT TARGET ${target_name})
        add_library(${target_name} INTERFACE)
    endif()

    target_compile_options(${target_name} INTERFACE
        -fsanitize=${SANITIZER_LIST}
        -fno-omit-frame-pointer
        -fno-optimize-sibling-calls
        -ffunction-sections
        -fdata-sections)

    foreach(SAN_OPTION ${SAN_OPTIONS})
        target_compile_options(${target_name} INTERFACE
            ${SAN_OPTION})
    endforeach()

    target_link_options(${target_name} INTERFACE
        -fsanitize=${SANITIZER_LIST})

    if(APPLE)
        target_link_options(${target_name} INTERFACE
            -Wl,-dead_strip
            -Wl,-undefined,dynamic_lookup)
    else()
        target_link_options(${target_name} INTERFACE
            -Wl,--gc-sections
            -Wl,--allow-multiple-definition)
    endif()
endfunction()
