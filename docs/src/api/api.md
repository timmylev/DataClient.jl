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
reload_configs
```

## Developer API

### Types

```@docs
DataClient.Store
DataClient.S3Store
DataClient.S3DB
DataClient.FFS
DataClient.Metadata
DataClient.S3DBMeta
DataClient.FFSMeta
DataClient.AWSUtils.FileCache
```

### Helper Functions

```@docs
DataClient.get_metadata
DataClient.write_metadata
DataClient.decode_type
DataClient.encode_type
DataClient.sanitize_type
DataClient.generate_s3_metadata_key
DataClient.generate_s3_file_key
DataClient.get_s3_file_timestamp
```

### Utility Functions

```@docs
DataClient.AWSUtils.s3_cached_get
DataClient.AWSUtils.s3_list_dirs
DataClient.unix2zdt
DataClient.utc_day_floor
```
