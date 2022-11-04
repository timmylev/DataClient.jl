# API

## API Index

```@index
Pages = ["api.md"]
```

## Public API

Refer to [Getting Started](@ref) in the Manual section for Public API code examples.

### List

```@docs
list_datasets
```

### Gather

```@docs
gather
```

### Insert

```@docs
insert
```

### Backends and Configs

Refer to [Configs and Backend](@ref) in the manual for more info about config files and backend stores.

```@docs
get_backend
reload_backend
```

## Developer API

### Store and StoreMetadata Types

```@docs
DataClient.Store
DataClient.S3Store
DataClient.S3DB
DataClient.FFS
DataClient.Metadata
DataClient.S3Meta
DataClient.S3DBMeta
DataClient.FFSMeta
```

### Index Types

```@docs
DataClient.Index
DataClient.TimeSeriesIndex
DataClient.PartitionSize
```

## Storage Format
```@docs
DataClient.StorageFormat
DataClient.load_df
DataClient.write_df
```

### Core Helper Functions (used in `gather` and `insert`)

```@docs
DataClient.get_metadata
DataClient.write_metadata
DataClient.gen_s3_file_key
DataClient.gen_s3_file_keys
DataClient.gen_s3_metadata_key
DataClient.create_partitions
DataClient.filter_df!
```

### Other Helper Functions

```@docs
DataClient.decode
DataClient.encode
DataClient.decode_type
DataClient.encode_type
DataClient.sanitize_type
DataClient.get_index
DataClient.get_storage_format
```

### Utility Functions and types

```@docs
DataClient.AWSUtils.FileCache
DataClient.AWSUtils.s3_cached_get
DataClient.AWSUtils.s3_list_dirs
DataClient.unix2zdt
```
