# Copyright 2020, Trail of Bits, Inc. All rights reserved.

function(add_util_library source_dir config)
    add_library(util-${config} STATIC
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/BitManipulation.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/Compiler.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/DefUse.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/DisjointSet.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/EqualitySet.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/FileManager.h"
        "${PROJECT_SOURCE_DIR}/include/drlojekyll/Util/Node.h"

        "${source_dir}/DefUse.cpp"
        "${source_dir}/DisjointSet.cpp"
        "${source_dir}/EqualitySet.cpp"
        "${source_dir}/FileManager.cpp"
        "${source_dir}/Node.cpp"
    )

    target_link_libraries(util-${config}
        PUBLIC settings-${config}
    )
endfunction(add_util_library)
