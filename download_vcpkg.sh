#!/bin/sh -eu

cur_dir=$(cd -- "$(dirname -- "$0")" && pwd -P)

# default arguments
vcpkg_commit="$(cat "${cur_dir}/vcpkg_commit.txt")"
vcpkg_url="https://github.com/microsoft/vcpkg.git"
vcpkg_directory="${cur_dir}/vcpkg"

# Loop through arguments and process them
for arg in "$@"
do
    case $arg in
        -h|--help)
        echo "Usage:"
        echo "  -c=<commit> | --vcpkg_commit=<commit>    Use specified commit"
        echo "  -u=<url>    | --vcpkg_url=<url>          Use specified url"
        echo "  -d=<dir>    | --vcpkg_directory=<dir>    Use specified directory"
        exit 0
        ;;
        -c=*|--vcpkg_commit=*)
        vcpkg_commit="${arg#*=}"
        shift
        ;;
        -u|--vcpkg_url)
        vcpkg_url="$2"
        shift
        shift
        ;;
        -d|--vcpkg_directory)
        vcpkg_directory="$2"
        shift
        shift
        ;;
    esac
done

echo "Setting up vcpkg from ${vcpkg_url}@${vcpkg_commit} in ${vcpkg_directory}"
echo ""

if [ ! -d "${vcpkg_directory}" ]; then
    git clone -n "${vcpkg_url}" "${vcpkg_directory}" > /dev/null 2>&1
fi
cd "${vcpkg_directory}"
if [ "$(git rev-parse HEAD)" != "${vcpkg_commit}" ]; then
    git checkout "${vcpkg_commit}"
fi
cd "${cur_dir}"

echo ""
echo "Done setting up vcpkg"
echo "Please run the following to install DrLojekyll dependencies"
echo ""
echo "  $ ${vcpkg_directory}/bootstrap-vcpkg.sh"
echo "  $ ${vcpkg_directory}/vcpkg install @vcpkg.txt"
echo ""
echo "To compile DrLojekyll with vcpkg dependencies, please add the following to your"
echo "CMake invocation:"
echo ""
echo "  -DCMAKE_TOOLCHAIN_FILE=${vcpkg_directory}/scripts/buildsystems/vcpkg.cmake"
echo ""
echo "See https://github.com/microsoft/vcpkg for more details regarding vcpkg"
