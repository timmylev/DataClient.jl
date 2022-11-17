using AWS.AWSExceptions: AWSException
using TimeZones: zdt2unix

######################################################################
#####   Type Definitions                                          ####
######################################################################

# just a handy alias
const ColumnTypes = Dict{String,Type}

"""
    PartitionSize

Chunk size options Enum for [`TimeSeriesIndex`](@ref)

# Enum Values
- HOUR
- DAY
- MONTH
- YEAR
"""
@enum PartitionSize HOUR DAY MONTH YEAR
const _partition_size = Dict(string.(instances(PartitionSize)) .=> instances(PartitionSize))
const _period = Dict(HOUR => Hour(1), DAY => Day(1), MONTH => Month(1), YEAR => Year(1))
PartitionSize(str::AbstractString)::PartitionSize = _partition_size[str]
value(key::PartitionSize) = _period[key]

"""
    Index

The abstract type `Index` used by stores.
"""
abstract type Index end

"""
    encode(index::Index)::Dict

Encodes an `Index` to a `Dict` such as it is easily JSON-serializable.
"""
function encode(index::Index)::Dict
    return Dict(
        "_type" => repr(typeof(index)),
        "_attr" => _encode_index(index),  # Implemented by all sub-types of Index
    )
end

"""
    decode(index::Index)::Dict

Decode an `Index` from a `Dict` back to its original type.
"""
function decode(::Type{Index}, enc::Dict)::Index
    type = getfield(@__MODULE__, Symbol(enc["_type"]))
    return _decode_index(type, enc["_attr"])  # Implemented by sub-types
end

"""
    TimeSeriesIndex

An `Index` option available for use by [`FFS`](@ref).

# Attributes
- `key`: The `DataFrame` column name to be used as the index, this column must be a
    `ZonedDateTime`.
- `partition_size`: Basically how much data a single s3 file in the backend should
    contain, expressed as a time period. Selecting a good partition size will depend a
    lot on the query pattern and how densely populated the dataset is across time. The
    goal is to mimize the number of backend connections (i.e. number of individual files
    transferred) while not retrieving excessive data that isn't actually required by
    the query.
"""
struct TimeSeriesIndex <: Index
    key::String
    partition_size::PartitionSize
end

function _encode_index(index::TimeSeriesIndex)::Dict
    return Dict("key" => index.key, "partition_size" => string(index.partition_size))
end

function _decode_index(::Type{TimeSeriesIndex}, enc::Dict)::TimeSeriesIndex
    return TimeSeriesIndex(enc["key"], PartitionSize(enc["partition_size"]))
end

"""
    S3Store

An abstract sub-type of [`Store`](@ref)
"""
abstract type S3Store <: Store end

"""
    S3DB

A concrete sub-type of [`S3Store`](@ref) that is used to represent the storage location
of S3DB data, i.e. the location in S3 where the Datafeeds Transmuters output data to.
S3DB stores are read-only, so one can perform [`list_datasets`](@ref) and 
[`gather`](@ref) operations but not [`insert`](@ref) operations. This also means that
the DataClient has no control over what format or structure the data is stored in, thus
must conform to the standards set by Datafeeds.
"""
struct S3DB <: S3Store
    bucket::String
    prefix::String
end

"""
    FFS

A concrete sub-type of [`S3Store`](@ref) that is used to represent the storage location
of a Market Sims Flat File Store, i.e. the location in S3 where manually derived
datasets (non-datafeeds) are stored. All datasets within a FFS are stored via the
DataClient's own [`insert`](@ref) function, therefore the DataClient has full control
over the implementation details of an FFS.
"""
struct FFS <: S3Store
    bucket::String
    prefix::String
end

"""
    S3Meta

An abstract sub-type [`Metadata`](@ref)
"""
abstract type S3Meta <: Metadata end

"""
    S3DBMeta

A concrete sub-type [`S3Meta`](@ref) that is used to encapsulate S3DB metadata.
"""
struct S3DBMeta <: S3Meta
    collection::String
    dataset::String
    store::S3DB
    timezone::TimeZone
    meta::Dict{String,Any}
end

"""
    FFSMeta

A concrete sub-type [`S3Meta`](@ref) that is used to encapsulate FFS metadata.
"""
Base.@kwdef struct FFSMeta <: S3Meta
    collection::String
    dataset::String
    store::FFS
    column_order::Vector{String}
    column_types::ColumnTypes
    timezone::TimeZone
    index::Index
    file_format::FileFormat
    compression::Union{Compression,Nothing}
    last_modified::ZonedDateTime
    details::Union{Nothing,Dict{String,String}} = nothing
end

Base.show(io::IO, ::MIME"text/plain", item::S3Meta) = Base.show(io, item)

# purely for aesthetic purposes
function Base.show(io::IO, item::S3Meta)
    lines = ["$(item.collection)-$(item.dataset)"]  # first line to print
    attrs = setdiff(propertynames(item), (:collection, :dataset))
    max_len = max(map(length, map(string, attrs))...)
    ending = []

    for p in attrs
        val = getfield(item, p)
        if isa(val, Dict)
            push!(ending, p)  # skip dicts for now, wait till end
        else
            push!(lines, Format.format("  {:$(max_len)s} : {:s}", p, val))
        end
    end

    # add dicts now
    for p in ending
        push!(lines, Format.format("  {:$(max_len)s} :", p))
        push!(lines, format_dict(getfield(item, p); offset=max_len + 5))
    end

    return print(join(lines, "\n"))
end

######################################################################
#####   Storing/Retrieving and Encoding/Decoding of Metadata      ####
######################################################################

"""
    get_metadata(coll::String, ds::String, store::S3Store)::S3Meta

Retrieves the metadata of a dataset from an [`S3Store`](@ref).
"""
function get_metadata(coll::AbstractString, ds::AbstractString, store::S3Store)::S3Meta
    s3_key = gen_s3_metadata_key(coll, ds, store)

    data = try
        file = @mock s3_cached_get(store.bucket, s3_key)
        JSON.parse(read(file, String))
    catch err
        if isa(err, AWSException) && err.code == "NoSuchKey"
            rethrow(MissingDataError(coll, ds))
        else
            throw(err)
        end
    end

    return _decode_meta(coll, ds, store, data)
end

# decodes S3DB metadata
function _decode_meta(
    coll::AbstractString, ds::AbstractString, store::S3DB, data::Dict
)::S3DBMeta
    return S3DBMeta(coll, ds, store, get_tz(coll, ds), data)
end

# decodes FFS metadata
function _decode_meta(
    coll::AbstractString, ds::AbstractString, store::FFS, data::Dict
)::FFSMeta
    column_types = Dict(k => decode_type(v) for (k, v) in pairs(data["column_types"]))
    compression =
        data["compression"] == "nothing" ? nothing : Compression(data["compression"])
    return FFSMeta(;
        collection=coll,
        dataset=ds,
        store=store,
        column_order=data["column_order"],
        column_types=column_types,
        timezone=TimeZone(data["timezone"]),
        index=decode(Index, data["index"]),
        file_format=FileFormat(data["file_format"]),
        compression=compression,
        last_modified=unix2zdt(data["last_modified"]),
        details=data["details"],
    )
end

"""
    write_metadata(metadata::S3Meta)

Writes the metadata of a dataset to an [`S3Store`](@ref) store.
"""
function write_metadata(meta::S3Meta)
    encoded = _encode_meta(meta)
    s3key = gen_s3_metadata_key(meta.collection, meta.dataset, meta.store)
    @mock s3_put(meta.store.bucket, s3key, JSON.json(encoded))
    return nothing
end

# encodes FFS metadata
function _encode_meta(meta::FFSMeta)::Dict
    return Dict(
        "column_order" => meta.column_order,
        "column_types" => Dict(k => encode_type(v) for (k, v) in meta.column_types),
        "timezone" => meta.timezone.name,
        "index" => encode(meta.index),
        "file_format" => string(meta.file_format),
        "compression" => string(meta.compression),
        "last_modified" => zdt2unix(Int, meta.last_modified),
        "details" => meta.details,
    )
end

########################################################################################
########################################################################################
####              Helper Functions Required by `gather` and `insert`                ####
##                                                                                    ##
## These functions are used to implement `gather` and `insert` for `S3Store`s (i.e.   ##
## `S3DB` and `FFS`):                                                                 ##
##    1. gen_s3_file_key                                                              ##
##    2. gen_s3_file_keys                                                             ##
##    3. filter_df!                                                                   ##
##    4. create_partitions                                                            ##
##                                                                                    ##
## They are simply generic implementations that call a lower level group of functions ##
## with index-specific implementations and dispatches. Any new indexes must implement ##
## these lower level functions. Refer to the implementation of the `TimeSeriesIndex`  ##
## below for more details.                                                            ##
##                                                                                    ##
##    1. _hash_key(indexed_val, index)::String                                        ##
##      -> The hash function that generates a hash for a given indexed value.         ##
##                                                                                    ##
##    2. _hash_keys(start, stop, index)::Vector{String}                               ##
##      -> Generates all hash keys that fall between indexed range start/stop         ##
##                                                                                    ##
##    3. _filter_df!(df, start, stop, index; hash_val)                                ##
##      -> Filters the input df for indexed range start/stop. Optionally provide the  ##
##         hash if a df was obtained from source file with that hash (optimisation)   ##
##                                                                                    ##
##    4. _create_partitions(df, index)::GroupedDataFrame                              ##
##      -> Creates partitions from the input df based on the index. This is used when ##
##         inserting the df into the backend store                                    ##
########################################################################################

# Datafeeds/S3DB constants
const S3DB_INDEX = TimeSeriesIndex("target_start", DAY)
const S3DB_FORMAT = FileFormats.CSV
const S3DB_COMPRESSION = FileFormats.GZ

"""
    get_index(meta::FFSMeta)::Index
    get_index(meta::S3DBMeta)::TimeSeriesIndex

Gets the [`Index`](@ref) from a dataset's [`S3Meta`](@ref)
"""
get_index(meta::S3DBMeta)::TimeSeriesIndex = S3DB_INDEX
get_index(meta::FFSMeta)::Index = meta.index

"""
    get_file_format(meta::FFSMeta)::FileFormat
    get_file_format(meta::S3DBMeta)::FileFormat

Gets the [`FileFormat`](@ref) from a dataset's [`S3Meta`](@ref)
"""
get_file_format(meta::S3DBMeta)::FileFormat = S3DB_FORMAT
get_file_format(meta::FFSMeta)::FileFormat = meta.file_format

"""
    get_file_compression(meta::FFSMeta)::Union{Compression,Nothing}
    get_file_compression(meta::S3DBMeta)::Union{Compression,Nothing}

Gets the [`Compression`](@ref) from a dataset's [`S3Meta`](@ref)
"""
get_file_compression(meta::S3DBMeta)::Union{Compression,Nothing} = S3DB_COMPRESSION
get_file_compression(meta::FFSMeta)::Union{Compression,Nothing} = meta.compression

"""
    gen_s3_file_key(indexed_val::Any, metadata::S3Meta)::String

Generates the s3 key for the file that contains data associated with the input
`indexed_val`. The type of `indexed_val` must be compatible with the index used in
`metadata`.
"""
function gen_s3_file_key(indexed_val::Any, metadata::S3Meta)::String
    hash_val = _hash_key(indexed_val, get_index(metadata))
    return _hash_val_to_s3_key(hash_val, metadata)
end

"""
    gen_s3_file_keys(start::Any, stop::Any, metadata::S3Meta)::Vector{String}

Similar to [`gen_s3_file_key`](@ref) but generates s3 keys for all files that fall
between the indexed range `start` and `stop`. The type of `start` and `stop` must be
compatible with the index used in `metadata`.
"""
function gen_s3_file_keys(start::T, stop::T, metadata::S3Meta)::Vector{String} where {T}
    index = get_index(metadata)
    hash_vals = _hash_keys(start, stop, index)
    return [_hash_val_to_s3_key(hash_val, metadata) for hash_val in hash_vals]
end

"""
    gen_s3_metadata_key(
        coll::AbstractString, ds::AbstractString, store::S3Store
    )::String

Generates the s3 key for a [`S3Store`](@ref) dataset metadata file.
"""
function gen_s3_metadata_key(
    coll::AbstractString, ds::AbstractString, store::S3Store
)::String
    return joinpath(store.prefix, coll, ds, "METADATA.json")
end

"""
    filter_df!(
        df::DataFrame,
        start::T,
        stop::T,
        metadata::S3Meta;
        s3_key::Union{AbstractString, Nothing} = nothing
    ) where {T}

Filters the input `DataFrame` by `start` and `stop`. The type of `start` and `stop`
must be compatible with the index used in `metadata`.

Optionally provide the source `s3_key` that is associated with the input `DataFrame`
(if applicable) for index-specific optimizatinos.
"""
function filter_df!(
    df::DataFrame,
    start::T,
    stop::T,
    metadata::S3Meta;
    s3_key::Union{AbstractString,Nothing}=nothing,
) where {T}
    if !isnothing(s3_key)
        hash_val = _s3_key_to_hash_val(s3_key, metadata)
        _filter_df!(df, start, stop, get_index(metadata), hash_val)
    else
        _filter_df!(df, start, stop, get_index(metadata))
    end

    return nothing
end

"""
    create_partitions(df::DataFrame, metadata::S3Meta)::GroupedDataFrame

Creates partition from the input `DataFrame` based on the `index`. This is used when
inserting the `DataFrame` into the backend store.
"""
function create_partitions(df::DataFrame, metadata::S3Meta)::GroupedDataFrame
    return _create_partitions(df, get_index(metadata))
end

function _hash_val_to_s3_key(hash_val::AbstractString, meta::S3Meta)::String
    file_ext = FileFormats.extension(get_file_format(meta), get_file_compression(meta))
    return joinpath(meta.store.prefix, meta.collection, meta.dataset, "$hash_val$file_ext")
end

function _s3_key_to_hash_val(s3_key::AbstractString, meta::S3Meta)::String
    key_prefix = joinpath(meta.store.prefix, meta.collection, meta.dataset)
    file_ext = FileFormats.extension(get_file_format(meta), get_file_compression(meta))
    return chop(s3_key; head=length(key_prefix) + 1, tail=length(file_ext))
end

########################################################################################
####  `TimeSeriesIndex` Implementations of the Following Required Functions:          ##
##    ->  _hash_key(...)                                                             ##
##    ->  _hash_keys(...)                                                            ##
##    ->  _filter_df!(...)                                                            ##
##    ->  _create_partitions(...)                                                     ##
########################################################################################

# The is fundamental hash function that is used to implement the `TimeSeriesIndex`
function _hash(zdt::ZonedDateTime, index::TimeSeriesIndex)::ZonedDateTime
    return floor(astimezone(zdt, tz"UTC"), value(index.partition_size))
end

function _hash_range(start::ZonedDateTime, stop::ZonedDateTime, index::TimeSeriesIndex)
    lower = _hash(start, index)
    upper = _hash(stop, index)
    return range(lower, upper; step=value(index.partition_size))
end

function _hash_key(zdt::ZonedDateTime, index::TimeSeriesIndex)::String
    start_day = _hash(zdt, index)
    year = Dates.year(start_day)
    return "year=$year/$(zdt2unix(Int, start_day))"
end

function _hash_keys(
    start::ZonedDateTime, stop::ZonedDateTime, index::TimeSeriesIndex
)::Vector{String}
    return [_hash_key(el, index) for el in _hash_range(start, stop, index)]
end

# reverts the hash key back into a ZDT
function _hash_key_to_zdt(hash_key::AbstractString, index::TimeSeriesIndex)::ZonedDateTime
    ts = split(hash_key, "/")[2]
    return unix2zdt(parse(Int, ts))
end

_convert(::Type{ZonedDateTime}, val::ZonedDateTime) = val
_convert(::Type{Int}, val::ZonedDateTime) = zdt2unix(Int, val)

function _filter_df!(
    df::DataFrame,
    start::ZonedDateTime,
    stop::ZonedDateTime,
    index::TimeSeriesIndex,
    hash_val::AbstractString,
)
    hash_zdt = _hash_key_to_zdt(hash_val, index)
    lower = _hash(start, index)
    upper = _hash(stop, index)

    if hash_zdt in (lower, upper)
        trace(LOGGER, "Filtering DataFrame for: '$hash_val'")
        _filter_df!(df, start, stop, index)
    elseif hash_zdt < lower || hash_zdt > upper
        trace(LOGGER, "Skipping filter and emptying DataFrame for '$hash_val")
        empty!(df)
    else
        trace(LOGGER, "Skipping filter for DataFrame '$hash_val")
    end

    return nothing
end

function _filter_df!(
    df::DataFrame, start::ZonedDateTime, stop::ZonedDateTime, index::TimeSeriesIndex
)
    # zdt index may be still represented as int in the df depending on at which stage
    # are we doing this filtering
    index_type_in_df = eltype(df[!, index.key])
    lower, upper = _convert(index_type_in_df, start), _convert(index_type_in_df, stop)

    filter_func(val) = val >= lower && val <= upper
    filter!(Symbol(index.key) => filter_func, df)

    return nothing
end

function _create_partitions(df::DataFrame, index::TimeSeriesIndex)::GroupedDataFrame
    groupkey(zdt) = zdt2unix(Int, _hash(zdt, index))
    # do not mutate the input df, also, as a results of this, the final GroupedDataFrame
    # will have an extra :group_key column. This if fine because we'll not include it
    # later on when inserting the data into the backend.
    df_temp = select(
        df, :, Symbol(index.key) => ByRow(groupkey) => :group_key; copycols=false
    )
    return groupby(df_temp, :group_key)
end
