This directory contains vendored dependencies; when adding a new dependency, make sure that the find_package handler behaves in a standard way so that the project can build either with or without DRLOJEKYLL_ENABLE_VENDORED_LIBRARIES=true.

# Third-party Dependencies

This directory contains "vendored" third-party dependencies, using submodules against the upstream repositories.

## Adding a new Dependency

To add a new vendored dependency:

0. Do we really need the third-party dependency? Is the license compatible? (only use MIT, BSD, Apache)
1. Create a new folder: `mkdir vendor/<library_name>`
2. Create the submodule: `git clone https://github.com/org/<library_name> vendor/<library_name>/src`
3. Optional: Enter the src folder that has been created, and checkout a known tag: `git fetch --tags && git checkout -b release_<vers> <tag_name>`
4. Add the submodule: `git add src`. This locks it at the known version we have just checked out
5. Determine whether to create a module or a config file by checking the upstream docs (are they using CONFIG for find_package?)
6. Create the config file (`vendor/configs/<library_name>Config.cmake`) or the module file (`vendor/modules/Find<library_name>.cmake`). Use the existing ones as templates
7. Create a CMakeLists.txt file under `vendor/<library_name>/CMakeLists.txt`. Use the existing ones as a starting point. Make sure the target names are correct. You may have to replicate ALIAS targets (example: grpc)
8. Update the [Third-party Dependency Inventory](#Third-party_Dependency_Inventory).

## Updating an existing dependency

1. Enter the submodule folder and fetch the new updates: `git fetch --all --tags`
2. Optional: checkout a known tag: ` git checkout -b release_<vers> <tag_name>`
3. Add the submodule: `git add src`
4. Configure the library: `cmake -S src -B build_folder`
5. Compare the existing options in `vendor/<library_name>/CMakeLists.txt` with the new ones: `ccmake build_folder`
6. Update options as necessary

## Third-party Dependency Inventory

Run the following command:

```
git submodule foreach bash -c 'git remote -v | grep fetch ; git fetch --tags > /dev/null 2>&1 ; ( git describe --tags || git rev-parse HEAD ) ; git show -s --format=%ci HEAD ; echo'
```

Sample output:

```
Entering 'vendor/flatbuffers/src'
origin  https://github.com/google/flatbuffers (fetch)
v2.0.0
2021-05-10 11:45:16 -0700

Entering 'vendor/googletest/src'
origin  https://github.com/google/googletest (fetch)
release-1.11.0
2021-06-11 10:42:26 -0700

Entering 'vendor/grpc/src'
origin  https://github.com/grpc/grpc (fetch)
v1.41.0-pre1-36-g8e4c14fbed
2021-09-22 22:21:46 -0700

Entering 'vendor/rapidcheck/src'
origin  https://github.com/emil-e/rapidcheck (fetch)
fatal: No names found, cannot describe anything.  <--- No tags defined upstream
33d15a858e3125f5af61a655f390f1cbc2f272ba
2021-07-02 13:54:10 +0200

Entering 'vendor/reproc/src'
origin  https://github.com/DaanDeMeyer/reproc.git (fetch)
v14.2.3
2021-08-08 12:18:47 +0100
```
