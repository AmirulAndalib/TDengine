aux_source_directory(src TSS_SRC)

add_library(tss STATIC ${TSS_SRC})
target_include_directories(
    tss
    PUBLIC "${TD_SOURCE_DIR}/include/libs/tss"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    tss
    PUBLIC common
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
