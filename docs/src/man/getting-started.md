# Getting Started

The 4 main APIs that you will be using are:
1. [`get_backend`](@ref) - Gets available backend stores
2. [`list_datasets`](@ref) - Lists available datasets
3. [`gather`](@ref) - Queries data from a target dataset
4. [`insert`](@ref) - Inserts new data into a new or existing dataset (Only supported for certain types of stores)

## Listing/Locating Datasets

`DataClient` works by connecting to a target data repository, known as a backend store.
There are multiple types/implementations of backend stores such as [`DataClient.S3DB`](@ref) and [`DataClient.FFS`](@ref), each having their own characteristics and use cases, though all of them share the same data access APIs.
An instance of a store must be registered before it can be accessed via the APIs.
There are a few centralized stores that are pre-registered as constants in the package (`DataClient.CENTRALIZED_STORES`) which makes them readily accessible.
Additional user-defined stores can be registered via a simply config file in the user's project directory.
We cover how to do this in [Inserting Datasets](#Inserting-Datasets) and [Configs and Backend](@ref).

The datasets available in each store are grouped by collection, which provides an additional level of dataset namespacing.

```julia
julia> using DataClient

# Check for available stores, if a config file is present with additional stores specified,
# they will appear here.
julia> get_backend()
OrderedCollections.OrderedDict{String, DataClient.Store} with 5 entries:
  "datafeeds"       => S3DB("invenia-datafeeds-output", "version5/aurora/gz/" ...
  "datafeeds-arrow" => S3DB("invenia-datafeeds-output", "version5/arrow/zst_lv22/day/" ...
  "public-data"     => FFS("invenia-private-datasets", "DataClient/ffs/arrow/zst/")
  ...

# List every dataset from every available store.
julia> list_datasets()
Dict{String, Dict{String, Vector{String}}} with 5 entries:
  "ercot-nda"       => Dict()
  "datafeeds"       => Dict("nyiso"=>["dayahead_load", "dayahead_marketwide", ...
  "datafeeds-arrow" => Dict("nyiso"=>["dayahead_load", "dayahead_marketwide", ...
  ...

# List every dataset from the 'datafeeds-arrow' store.
julia> list_datasets("datafeeds-arrow")
Dict{String, Vector{String}} with 8 entries:
  "nyiso"   => ["dayahead_load", "dayahead_marketwide", "dayahead_price", "realtime_load", ...
  "spp"     => ["day_ahead_binding_constraints", "dayahead_load", "dayahead_marketwide", ...
  ...

# List every dataset from the 'nyiso' collection in the 'datafeeds-arrow' store.
julia> list_datasets("datafeeds-arrow", "nyiso")
 6-element Vector{String}:
  "dayahead_load"
  "dayahead_marketwide"
  ...
```

Notice that there are two types of store available, [`DataClient.S3DB`](@ref) and [`DataClient.FFS`](@ref).
Basically, `S3DB` is a read-only store ([`insert`](@ref) operations will fail) because data in `S3DB` stores are generated and maintained by external systems such as the [Transmuter](https://gitlab.invenia.ca/invenia/Datafeeds/Transmuters) or the [S3DBConverter](https://gitlab.invenia.ca/invenia/Datafeeds/S3DBConverter), and we want to be sure to _not_ make any modification to such data.
DataClient simply "plugs in" and reads what is available.
Data within `FFS` stores on the other hand, are inserted using DataClient itself, where DataClient's native storage implementation applies, so both reads and writes are supported (provided that the caller has the appropriate IAM access).

## Gathering Datasets

When [`gather`](@ref)ing a dataset, the `collection` and `dataset` must be specified, but `store_id` is optional.
When `store_id` is not specified, the order from [`get_backend`](@ref) is used to iteratively search through all available stores until the first store containing the requested `collection` and `dataset` is found (notice that `get_backend()` returns an `OrderedDict`).
See [Configs and Backend](@ref) for more information about how the search order is determined.
It is _always recommended_ to provide a `store_id` for better performance and to avoid ambiguity.

```julia
julia> using DataClient
julia> using TimeZones

julia> start = ZonedDateTime(2020, 2, 13, tz"UTC")
julia> stop = ZonedDateTime(2020, 2, 13, 23, tz"UTC")

# Iterates through all available backend stores until the dataset is found.
# In this case, the "datafeeds" store is used.
julia> df = gather("spp", "realtime_price", start, stop)
71496×9 DataFrame

# Specifically queries the 'datafeeds-arrow' backend store.
julia> df = gather("spp", "realtime_price", start, stop, "datafeeds-arrow")
71496×9 DataFrame
```

### Dataset Metadata

The `DataFrame` returned by [`gather`](@ref) contains `S3DB` (or `FFS`) metadata attached to it, which can be accessed via the Tables.jl metadata interface.
```julia
julia> meta = metadata(df)["metadata"]
spp-realtime_price
  store    : DataClient.S3DB("invenia-datafeeds-output", "version5/arrow/zst_lv22/day/", ...
  timezone : tz"America/Chicago"
  meta     :
             "type_map"  =>
               "mlc"           => "float"
               "target_end"    => "int"
               "tag"           => "str"
               "lmp"           => "float"
               "mcc"           => "float"
               "release_date"  => "int"
               "target_bounds" => "int"
               "target_start"  => "int"
               "node_name"     => "str"
             "superkey"  => Any["release_date", "target_start", "target_end", "node_name", "tag"]
             "value_key" => Any["lmp", "mlc", "mcc"]
             "tags"      =>
               "real_time_month"   =>
                 "time_zone"        => "America/Chicago"
                 "content_offset"   => 2678400
                 "content_interval" => 2678400
                 "publish_offset"   => 1339200
                 ...
```

### Caching
More info can be found in the [`gather`](@ref) docs but by default, an ephemeral file cache is automatically instantiated for each Julia session to cache downloaded S3 files.
This cache can be configured differently via a config file or environment variable.
* `DATACLIENT_CACHE_DIR` (String): The path to a local directory that is used as the cache. Files cached here will be persistent (not removed at the end of the julia session) and can be reused across sessions.
* `DATACLIENT_CACHE_SIZE_MB` (Int):  The max cache size in MB before older files are removed. The default is 20,000 MB.
* `DATACLIENT_CACHE_EXPIRE_AFTER_DAYS` (Int): Files in the custom cache dir (if specified) that are older than this period will be removed during initialization. The default is 90 days.
* `DATACLIENT_CACHE_DECOMPRESS` (Bool):  Whether or not to decompress S3 files before caching. The default is `true`.

The recommended way of configuring the above is by creating a config file in your project directory, this makes your configs persistent and gives you easy access to view/modify to them (see [Configs and Backend](@ref) for more information):
```sh
cat > configs.yaml <<- EOF
DATACLIENT_CACHE_DIR: "./cache/"
DATACLIENT_CACHE_SIZE_MB: 50000
EOF
```

Using environment variables via the shell is also possible and will override any config file variables if there is a conflict:
```sh
export DATACLIENT_CACHE_DIR="./cache/"
export DATACLIENT_CACHE_SIZE_MB=50000
```

If setting environment variables in Julia, **be sure to set them before calling the `gather` function.** because that is when the cache is initialized.
While DataClient supports reloading configs (eg. used in [`reload_backend`](@ref)), but re-initializing the cache is not currently supported.
```julia
ENV["DATACLIENT_CACHE_DIR"] = "./cache/"
ENV["DATACLIENT_CACHE_SIZE_MB"] = 50000
```

### Timezone-naive Queries
While _all_ stored datasets use a `ZonedDateTime` column as the index (see [Indexing](#Note-About-Indexing)), queries using a tz-naive `Date` (for 24-hrs of data) and a pair of `DateTime`s (start and end) are also supported for compatibility with certain projects.
This is simply for convenience, the relevant market's timezone (defined as a constant internally) will be attached to the tz-naive `DateTime` to form a `ZonedDateTime` internally before the query runs.
This will fail if the input `DateTime` is an ambiguous or non-existent datetime (due to DST transitions) for the relevant timezone.

```julia
julia> using Dates

julia> date = Date(2020, 3, 13)
julia> df = gather("spp", "realtime_price", date, "datafeeds-arrow")
julia> eltype(df.target_start)
DateTime

julia> start = DateTime(2020, 3, 13)
julia> stop = DateTime(2020, 3, 13, 23)
julia> df = gather("spp", "realtime_price", start, stop, "datafeeds-arrow")
julia> eltype(df.target_start)
DateTime

# March 2020 DST happens on the 8th for SPP's timezone
julia> df = gather("spp", "realtime_price", DateTime(2020, 3, 8, 2), stop, "datafeeds-arrow")
ERROR: NonExistentTimeError: Local DateTime 2020-03-08T02:00:00 does not exist within America/Chicago
```

Notice that the datetime columns in the resulting `DataFrame` also has its timezone stripped.
Note that _the datetimes were NOT converted into UTC before the timezone was stripped._
This may be problematic when querying for data that span the fall DST transitions because of the ambiguity caused by the repeated hour.
To retain the timezones in the resulting data, a `strip_tz=false` argument must be specified.

```julia
julia> df = gather("spp", "realtime_price", start, stop, "datafeeds-arrow"; strip_tz=false)
julia> eltype(df.target_start)
ZonedDateTime
```

Using tz-naive `DateTime`s should generally be avoided because it is bad practice to represent non-UTC datetimes using a `DateTime` for obvious reasons.
There is no performance benefit too in this case as is it simply supported for compatibility reasons with certain projects.

## Inserting Datasets

The [`insert`](@ref) operation is only supported for [`DataClient.FFS`](@ref)-type stores.
In this example, we'll store datasets in a custom `FFS` store (not one of the default centralized ones).
This requires the store to be defined in the config file.

```sh
cat > configs.yaml <<- EOF
additional-stores:
  - tim-personal: ffs:s3://tim-test-only/data-client-demo/
EOF
```

#### Note About Indexing
When storing a dataset, a column in the input `DataFrame` must be available for use as the dataset index.
Currently, the only available index type is the [`TimeSeriesIndex`](@ref) which **requires that the indexed column be of type `ZonedDateTime`.**
If no index is specified, the index defaults to `TimeSeriesIndex("target_start", DAY)`, which will error if such a column doesn't exist or is not a `ZonedDateTime`.
Experiments show that for most of our query patterns and use-cases, sticking to a `DAY` partition yields the best overall performance.
See [`DataClient.PartitionSize`](@ref) for supported partition sizes.

```julia
julia> using DataClient
julia> using DataFrames
julia> using TimeZones

# Read in from our config file
julia> store_id = "tim-personal"
julia> get_backend()[store_id]
DataClient.FFS("tim-test-only", "data-client-demo/")

# The store is currently empty
julia> list_datasets(store_id)
Dict{String, Vector{String}}()

# Generate a new dataset
# For convenience, we'll retrieve a dataset from DataFeeds
julia> df = gather(
    "spp",
    "realtime_price",
    ZonedDateTime(2020, 1, 1, tz"UTC"),
    ZonedDateTime(2020, 2, 1, tz"UTC"),
    "datafeeds-arrow",
)
2219301×9 DataFrame
julia> df_col_types = Dict(names(df) .=> eltype.(eachcol(df)))
Dict{String, DataType} with 9 entries:
  "mlc"           => Float64
  "target_end"    => ZonedDateTime
  "tag"           => String
  "lmp"           => Float64
  "mcc"           => Float64
  "release_date"  => ZonedDateTime
  "target_bounds" => String3
  "target_start"  => ZonedDateTime
  "node_name"     => String

# Now insert the dataset
julia> insert(
    "my-collection",
    "my-dataset",
    df,
    store_id;
    details=Dict("description" => "Copied over from datafeeds"),
    column_types=Dict("tag" => Union{Missing, String}),
    index=TimeSeriesIndex("target_start", DAY),
    file_format=FileFormats.ARROW,
    compression=FileFormats.ZST,
)

# A new dataset now exists:
julia> list_datasets(store_id)
Dict{String, Vector{String}} with 1 entry:
  "my-collection" => ["my-dataset"]

# Retrieve a portion of the dataset:
julia> df_new = gather(
    "my-collection",
    "my-dataset",
    ZonedDateTime(2020, 1, 12, tz"UTC"),
    ZonedDateTime(2020, 1, 17, tz"UTC"),
    store_id,
)
360459×9 DataFrame

# Inspect the dataset metadata
julia> meta = metadata(df_new)["metadata"]
my-collection-my-dataset
  store         : DataClient.FFS("tim-test-only", "data-client-demo/")
  column_order  : ["release_date", "target_start", "target_end", "target_bounds", "node_name", "tag", "lmp", "mlc", "mcc"]
  timezone      : tz"America/Chicago"
  index         : TimeSeriesIndex("target_start", DAY)
  file_format   : ARROW
  compression   : ZST
  last_modified : ZonedDateTime(2022, 12, 1, 15, 33, 41, tz"UTC")
  column_types  :
                  "mlc"           => AbstractFloat
                  "target_end"    => ZonedDateTime
                  "tag"           => Union{Missing, String}
                  "lmp"           => AbstractFloat
                  "mcc"           => AbstractFloat
                  "release_date"  => ZonedDateTime
                  "target_bounds" => AbstractString
                  "target_start"  => ZonedDateTime
                  "node_name"     => AbstractString
  details       :
                  "description" => "Copied over from datafeeds"
```
A few things to note here are:
    * File format options: [`FileFormats.FileFormat`](@ref)
    * Compression options: [`FileFormats.Compression`](@ref)
    * When creating a new dataset, a type map for the dataset columns is automatically generated based on the input `DataFrame` which will be registered in the backend. This type map is used to validate future insertions of new data into the dataset. The `column_types` keyword argument can be used to overwrite the default generated types, which can be used to tighten or loosen the data type restriction for future insertions.

Refer to the [`insert`](@ref) function docs for more in-depth documentation.
