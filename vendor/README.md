# Third-party Dependencies

This directory contains "vendored" third-party dependencies, in which a
copy of the dependency source code has been physically added to the Dr.
Lojekyll repository, and is built as part of that.

Note, we avoid making modifications to vendored third-party
dependencies, since doing so impedes the ability to update to newer
versions of those dependencies in the future.  (FIXME: cross this bridge
when we come to it...)

## Adding a New Dependency

To add a new vendored dependency:

0. Do we really need the third-party dependency?
1. Add a copy of the unmodified source of the dependency to a subdirectory under `vendor`.
2. Update the [Third-party Dependency Inventory](#Third-party_Dependency_Inventory).
3. Make an explicit note about the provenance of the dependency in your commit message.

## Third-party Dependency Inventory

| Name          | Website                              | Version                                                                                                             | Release Date    | License      | Purpose                |
| ----          | -------                              | -------                                                                                                             | ------------    | -------      | -------                |
| Google Test   | https://github.com/google/googletest | 1.10.0 from commit [703bd9ca](https://github.com/google/googletest/commit/703bd9caab50b139428cea1aaff9974ebee5742e) | October 3, 2019 | BSD 3-Clause | Unit Testing           |
| RapidCheck    | https://github.com/emil-e/rapidcheck | Commit [142c2d12](https://github.com/emil-e/rapidcheck/commit/142c2d124e34fabf00d673d76ae520c37319e2ac)             | May 4, 2020     | BSD 2-Clause | Property-based Testing |
| nlohmann_json | https://github.com/nlohmann/json     | 3.9.1 from commit [db78ac1d](https://github.com/nlohmann/json/commit/db78ac1d7716f56fc9f1b030b715f872f93964e4)      | August 6, 2020  | MIT License  | JSON Avro Schema       |
