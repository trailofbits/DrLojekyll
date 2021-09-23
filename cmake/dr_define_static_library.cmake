# TODO(bjl): add a comment explaining what is this function does, and why it exists?
function(dr_define_static_library libname)
    set(options)
    set(oneValueArgs CURDIR)
    set(multiValueArgs SOURCES PUBLIC_HEADERS DEPENDENCIES PRIVATE_DEPS)
    cmake_parse_arguments(CURLIB
        "${options}"
        "${oneValueArgs}"
        "${multiValueArgs}"
        ${ARGN})

    # Create our library
    add_library(${libname} STATIC
        ${CURLIB_PUBLIC_HEADERS}
        ${CURLIB_SOURCES}
    )
    set_target_properties(${libname} PROPERTIES LINKER_LANGUAGE CXX)

    add_library(${PROJECT_NAME}::${libname} ALIAS ${libname})

    if(DRLOJEKYLL_ENABLE_SANITIZERS)
        target_link_libraries(${libname} PUBLIC
            drlojekyll_sanitizers
        )
    endif()

    if(CURLIB_DEPENDENCIES)
        target_link_libraries(${libname}
            PUBLIC ${CURLIB_DEPENDENCIES}
        )
    endif()
    if(CURLIB_PRIVATE_DEPS)
        target_link_libraries(${libname}
            PRIVATE ${CURLIB_PRIVATE_DEPS}
        )
    endif()

    target_link_libraries(${libname}
        PUBLIC  settings_public
        PRIVATE settings_private
    )

    set_target_properties(${libname} PROPERTIES PUBLIC_HEADER "${CURLIB_PUBLIC_HEADERS}")
    target_include_directories(${libname} PUBLIC
      $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>
      PRIVATE ${CURLIB_CURDIR}
    )
    string(TOLOWER ${PROJECT_NAME} lower_project_name)

    if(DRLOJEKYLL_ENABLE_INSTALL)
        install(
        TARGETS ${libname}
        EXPORT "${PROJECT_NAME}Targets"
        RUNTIME
            DESTINATION "bin"
        LIBRARY
            DESTINATION "lib/${lower_project_name}"
        ARCHIVE
            DESTINATION "lib/${lower_project_name}"
        PUBLIC_HEADER
            DESTINATION "include/${lower_project_name}/${libname}"
        )
    endif()
endfunction()
