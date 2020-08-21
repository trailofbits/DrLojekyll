if(ENABLE_LIBFUZZER)
    add_executable(fuzzer
        Fuzzer.cpp)
    target_link_libraries(fuzzer PRIVATE
        settings
        settings-fuzz-target
        Util
        Display
        Lex
        Parse
        Version
    )
endif()
