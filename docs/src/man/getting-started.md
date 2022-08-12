# Getting Started

## Checking for Available Stores
Centralized stores such as S3DB (Datafeeds) are hard-coded in the package itself thus available by default.
Additional stores such as personal S3 buckets can be added via a config file, see [Configs and Backend](@ref) for more details.
```julia
julia> using DataClient

# By default, the DataClient checks `pwd()` for `configs.yaml` and loads it in if it exists.
# This example shows that the config file does not exist, which is not a problem because
# default configs exist.
julia> stores = get_backend()
[info | DataClient]: Config file '/Users/timmy/Invenia/DataClient.jl/configs.yaml' is not available, using default stores.
OrderedCollections.OrderedDict{String, DataClient.Store} with 1 entry:
  "datafeeds" => S3DB("invenia-datafeeds-output", "version5/aurora/gz/")

julia> store = get_backend("datafeeds")
DataClient.S3DB("invenia-datafeeds-output", "version5/aurora/gz/")

# Now say you've just added or modified the config file in the default config file path,
# an explicit reload is required for the config to take effect (in the same Julia session).
julia> reload_configs()

# If you are using a non-default config file path, simply specify the path. For example:
julia> reload_configs(joinpath("custom-path", "custom-file-name.yaml"))

# The updated configs now shows up.
julia> stores = get_backend()
OrderedCollections.OrderedDict{String, DataClient.Store} with 2 entries:
  "datafeeds"    => S3DB("invenia-datafeeds-output", "version5/aurora/gz/")
  "tim-personal" => FFS("tim-buck", "test-insert/")
```

## Listing Datasets
Each backend store is completely isolated from all other stores, making them the first level of namespace-ing for datasets.
Collections provide an additional dataset namespace because datasets are grouped into collections and each store can have multiple collections.
Therefore, the true 'path' of a datasets can be thought of as `<store-id>.<collection>.<dataset>`.
```julia
julia> using DataClient

# List every dataset from every available store.
julia> list_datasets()
Dict{String, Dict{String, Vector{String}}} with 2 entries:
  "datafeeds"    => Dict("nyiso"=>["dayahead_load", "dayahead_marketwide", ....
  "tim-personal" => Dict("nyiso"=>["realtime_price"])

# List every dataset from the 'datafeeds' store.
julia> list_datasets("datafeeds")
Dict{String, Vector{String}} with 8 entries:
  "nyiso"   => ["dayahead_load", "dayahead_marketwide", "dayahead_price", "realtime_load", ...
  "spp"     => ["day_ahead_binding_constraints", "dayahead_load", "dayahead_marketwide", ...
 ...

# List every dataset from the 'nyiso' collection in the 'datafeeds' store.
julia> list_datasets("datafeeds", "nyiso")
 6-element Vector{String}:
  "dayahead_load"
  "dayahead_marketwide"
 ...
```

## Gathering Datasets
When gathering a dataset, the `collection` and `dataset` must be specified, but `store_id` is optional.
When `store_id` is not specified, the order from `get_backend()` is used to iteratively search through all available stores until the first store containing the requested `collection.dataset` is found (notice that `get_backend()` returns an `OrderedDict`).
See [Configs and Backend](@ref) for more information about how the search order is determined.
```julia
julia> using DataClient
julia> using TimeZones

# Iterates through all available backend stores until data is found.
julia> df = gather(
    "spp",
    "realtime_price",
    ZonedDateTime(2020, 2, 13, tz"UTC"),
    ZonedDateTime(2020, 2, 13, 23, tz"UTC"),
)
71496×9 DataFrame

# Specifically checks the 'datafeeds' backend store for the target data.
julia> df = gather(
    "spp",
    "realtime_price",
    ZonedDateTime(2020, 2, 13, tz"UTC"),
    ZonedDateTime(2020, 2, 13, 23, tz"UTC"),
    "datafeeds",
)
71496×9 DataFrame
```

## Inserting Datasets
The insert operation is only supported for [`DataClient.FFS`](@ref)-type stores.
The current implementation of `DataClient.FFS` requires that the input `DataFrame` contain a `target_start` column of type `ZonedDateTime`.
```julia
julia> using DataClient
julia> using DataFrames
julia> using TimeZones

# We have 2 registered stores:
julia> get_backend()
OrderedCollections.OrderedDict{String, DataClient.Store} with 2 entries:
  "datafeeds"    => S3DB("invenia-datafeeds-output", "version5/aurora/gz/")
  "tim-personal" => FFS("tim-buck", "test-insert/")

# The FFS store is currently empty:
julia> list_datasets("tim-personal")
Dict{String, Vector{String}}()

# Create a new dataset:
julia> new_dataset = DataFrame(
    target_start=[
        ZonedDateTime(2020, 1, 1, 1, tz"America/New_York"),
        ZonedDateTime(2020, 1, 1, 2, tz"America/New_York"),
        ZonedDateTime(2020, 1, 1, 3, tz"America/New_York"),
    ],
    lmp=[11.1, 11.2, 11.3]
)

# Insert the dataset:
julia> insert("my-collection", "my-dataset", new_dataset, "tim-personal")

# A new dataset now exists:
julia> list_datasets("tim-personal")
Dict{String, Vector{String}} with 1 entry:
  "my-collection" => ["my-dataset"]

# Retrieve the dataset:
julia> df = gather(
    "my-collection",
    "my-dataset",
    ZonedDateTime(2020, 1, 1, 1, tz"America/New_York"),
    ZonedDateTime(2020, 1, 1, 3, tz"America/New_York"),
    "tim-personal",
)
3×2 DataFrame
 Row │ target_start               lmp     
     │ ZonedDateTi…               Float64 
─────┼────────────────────────────────────
   1 │ 2020-01-01T01:00:00-05:00     11.1
   2 │ 2020-01-01T02:00:00-05:00     11.2
   3 │ 2020-01-01T03:00:00-05:00     11.3
```
