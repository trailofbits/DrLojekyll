if(ENABLE_LIBFUZZER)
    add_executable(drlojekyll-parser-fuzzer
        ParserFuzzer.cpp)
    target_link_libraries(drlojekyll-parser-fuzzer PRIVATE
        settings_public
        settings_private
        settings_fuzz_target
        Util
        Display
        Lex
        Parse
        Version
    )

    add_executable(drlojekyll-backend-fuzzer
        BackendFuzzer.cpp)
    target_link_libraries(drlojekyll-backend-fuzzer PRIVATE
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
