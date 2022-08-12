# Developer Notes

## Repo Structure
The `src/` directory is structured as such:
* `src/AWSUtils/`: A sub-module for AWS-related utils that is general purpose and independent of the main module.
* `src/common.jl`: Custom data types and simple utility/helper functions that is used across multiple public APIs.
* `src/configs.jl`: Config file loading and parsing.
* `src/exceptions.jl`: Custom exceptions.
* The Public APIs (all 3 are independent of each other, but rely on the above):
    * `src/gather.jl`: The [`gather`](@ref) API.
    * `src/insert.jl`: The [`insert`](@ref) API.
    * `src/list.jl`: The [`list_datasets`](@ref) API.

The `test/` directory mimics the structure in `src/`, where each test file maps to a corresponding src file.
Test data are stored in `test/files/`.
