#!/bin/sh -eu

cur_dir=$(cd -- "$(dirname -- "$0")" && pwd -P)
# Hacky realpath
repo_root="$( cd "${cur_dir}/.." ; pwd -P )"
# Should be populated when running in CI
CI=${CI:-}

# default arguments
vcpkg_commit="$(cat "${repo_root}/vcpkg_commit.txt")"
vcpkg_url="https://github.com/microsoft/vcpkg.git"
vcpkg_directory="${repo_root}/vcpkg"

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

# First clone the repo, but don't check out any files
if [ ! -d "${vcpkg_directory}" ]; then
    git clone -n "${vcpkg_url}" "${vcpkg_directory}"
fi

cd "${vcpkg_directory}"
# Corner case to detect if vcpkg commit is at HEAD of master and not checked out
if [ "$(git rev-parse HEAD)" != "${vcpkg_commit}" ] || [ "$(find . -maxdepth 1 | wc -l)" -le 5 ]; then
    git fetch
    git checkout "${vcpkg_commit}"
    if [ -z "${CI}" ]; then
        "${vcpkg_directory}/bootstrap-vcpkg.sh"
        cd "${repo_root}"
        "${vcpkg_directory}/vcpkg" install @vcpkg.txt
        "${vcpkg_directory}/vcpkg" upgrade --no-dry-run
    fi
fi
cd "${repo_root}"

echo ""
echo "********************************************************************************"
echo ""
echo "To compile DrLojekyll with vcpkg dependencies, please add the following to your"
echo "CMake invocation:"
echo ""
echo "  -DCMAKE_TOOLCHAIN_FILE=${vcpkg_directory}/scripts/buildsystems/vcpkg.cmake"
echo ""
echo "See https://github.com/microsoft/vcpkg for more details regarding vcpkg"
