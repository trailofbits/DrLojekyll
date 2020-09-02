if(ENABLE_LIBFUZZER)
    add_executable(fuzzer
        Fuzzer.cpp)
    target_link_libraries(fuzzer PRIVATE
        settings_public
        settings_private
        settings_fuzz_target
        Util
        Display
        Lex
        Parse
        Version
    )
endif()
