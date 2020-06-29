# Copyright 2019, Trail of Bits, Inc. All rights reserved.

function(rel source_dir config)
    add_library(rel-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Rel/Format.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Rel/Query.h"
      
        "${source_dir}/Query.h"
        
        "${source_dir}/Aggregate.cpp"
        "${source_dir}/Build.cpp"
        "${source_dir}/Column.cpp"
        "${source_dir}/Connect.cpp"
        "${source_dir}/Constant.cpp"
        "${source_dir}/Constraint.cpp"
        "${source_dir}/Differential.cpp"
        "${source_dir}/Format.cpp"
        "${source_dir}/Input.cpp"
        "${source_dir}/Insert.cpp"
        "${source_dir}/Join.cpp"
        "${source_dir}/KVIndex.cpp"
        "${source_dir}/Map.cpp"
        "${source_dir}/Merge.cpp"
        "${source_dir}/Optimize.cpp"
        "${source_dir}/Query.h"
        "${source_dir}/Query.cpp"
        "${source_dir}/Select.cpp"
        "${source_dir}/Stream.cpp"
        "${source_dir}/Tuple.cpp"
        "${source_dir}/View.cpp")

    target_link_libraries(rel-${config} PUBLIC
        sema-${config})
endfunction(rel)
