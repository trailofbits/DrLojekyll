# Copyright 2020, Trail of Bits, Inc. All rights reserved.

function(add_dataflow_library source_dir config)
    add_library(dataflow-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Format.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/DataFlow/Query.h"

        "${source_dir}/Optimize.h"
        "${source_dir}/Query.h"

        "${source_dir}/Aggregate.cpp"
        "${source_dir}/Build.cpp"
        "${source_dir}/Column.cpp"
        "${source_dir}/Columns.cpp"
        "${source_dir}/Condition.cpp"
        "${source_dir}/Connect.cpp"
        "${source_dir}/Constant.cpp"
        "${source_dir}/Constraint.cpp"
        "${source_dir}/DeadFlowElimination.cpp"
        "${source_dir}/Differential.cpp"
        "${source_dir}/Format.cpp"
        "${source_dir}/IO.cpp"
        "${source_dir}/Insert.cpp"
        "${source_dir}/Join.cpp"
        "${source_dir}/KVIndex.cpp"
        "${source_dir}/Map.cpp"
        "${source_dir}/Merge.cpp"
        "${source_dir}/Optimize.cpp"
        "${source_dir}/Query.cpp"
        "${source_dir}/Select.cpp"
        "${source_dir}/Stream.cpp"
        "${source_dir}/Tuple.cpp"
        "${source_dir}/View.cpp"
    )

    target_link_libraries(dataflow-${config}
        PUBLIC
            settings-${config}
            display-${config}
            lex-${config}
            parse-${config}
            util-${config}
    )
endfunction()
