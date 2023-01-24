# Configs and Backend
The config file is a user-defined `.yaml` file that supplies optional configurations to the DataClient.
A sample config file can be found in [DataClient.jl/configs-example.yaml](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl).

To use one, simply add your configs to a file, name it `configs.yaml`, and place it in your current working directory (the `pwd()` of your Julia process or root level of your application).
This is the default load path the DataClient uses to check for custom configs.

## How The File is Loaded
The config file is loaded in automatically when any of the supported operations are called ([`get_backend`](@ref), [`list_datasets`](@ref), [`gather`](@ref), and [`insert`](@ref)).
Once loaded in for the first time, it is cached in memory for future use.
So, making manual changes to the config file will not take immediate effect unless the Julia session is restarted or [`reload_backend`](@ref) is called (which in turn calls `DataClient.Configs.reload_configs()` in the background).

A `reload_backend` call is also required if using a custom config file path that is not `joinpath(pwd(), "configs.yaml")`.

## Configurables

### Adding Additional Backend Stores
The optional `additional-stores` config allows you to register any number of additional stores to the DataClient such that they can be referenced using a simple store id instead of the full URI.
For example:
```yaml
additional-stores:
  - my-ffs: ffs:s3://my-ffs-bucket/
  - my-ffs-2: ffs:s3://my-ffs-bucket-2/my-prefix-1/
  - my-ffs-2b: ffs:s3://my-ffs-bucket-2/my-prefix-2/
  - staging-datafeeds: s3db:s3://datafeeds-staging-bucket/bucket-prefix/
```
Notice that the store URI is prefixed with a storage type and we currently only support two storage types:
- `s3db` : [`DataClient.S3DB`](@ref) is used to represent storage locations (s3 buckets) that contain data (s3 files) that is transmuted by Datafeeds. This includes data that is copied directly from the Datafeeds or data that has undergone some transformations or conversions (eg. via [S3DBConverters](https://gitlab.invenia.ca/invenia/Datafeeds/S3DBConverter)). This store type is read-only thus users will not be able to perform [`insert`](@ref) operations. The purpose of this store is to allow users to read Datafeeds or Datafeeds-like data.
- `ffs` : [`DataClient.FFS`](@ref) is DataClient's native storage implementation, where all operations are supported and all data contained in this store was initially inserted and formatted using DataClient's [`insert`](@ref) API. The purpose of this store is to be a database, where users get to store and retrieve data (using DataClient's APIs) as they would with a typical database system.

The order in which you specify the additional stores is important because **this is the same order that the [`gather`](@ref) operation uses to search for datasets** when a store id is not provided.

#### S3DB URI Schemeq
The first component of `FFS` store URIs always begin with `ffs:`, but this is not the case for `S3DB` stores.
Given that DataClient has no control over how `S3DB` data is stored/formatted nor what metadata is made available, certain information about the store must be provided in the URI in order for data to be read correctly, in particular, the file format, compression format, and partition size.
The following are some valid examples:
* `s3db:`: A shorthand for `s3db-csv-gz-day`
* `s3db-arrow-zst-day:`
* `s3db-arrow-lz4-month:`
* `s3db-csv-gz-year:`

`S3DB` URIs must be in the format `s3db-format-compression-partition:`, where format, compression, and partition represents how the S3DB data is actually stored.


### Prioritizing Additional Stores over Centralized Stores
If a store id is not provided when using the [`gather`](@ref) operation, the default behavior is to first check any hard-coded centralized stores before checking the user-defined `additional-stores`.
This behavior can be switched around by setting the `prioritize-additional-stores` config.
```yaml
prioritize-additional-stores: True
```

### Disabling Centralized Stores
The optional `disable-centralized` config allows you to disable any hard-coded centralized stores and only use the user-defined `additional-stores`.
```yaml
disable-centralized: True
```

### Cache Configs When Gathering Data
Refer to [`gather`](@ref).


[`DataClient.S3DB`](@ref) is used to represent storage locations (s3 buckets) that contain data (s3 files) that is transmuted by Datafeeds.
This includes data that is copied directly from the Datafeeds or data that has undergone some transformations or conversions (eg. via [S3DBConverters](https://gitlab.invenia.ca/invenia/Datafeeds/S3DBConverter)).
This store type is read-only thus you will not be able to perform [`insert`](@ref) operations.
