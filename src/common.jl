using AWS.AWSExceptions: AWSException
using TimeZones: zdt2unix

"""
    Store

The abstract type `Store` where all other stores are sub-typed from.
"""
abstract type Store end

"""
    S3Store

An abstract sub-type of [`Store`](@ref) and super-type to all S3-based stores.
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
    Metadata

The abstract type for a dataset's metadata.
"""
abstract type Metadata end

"""
    S3DBMeta

Metadata for a [`S3DB`](@ref) dataset.
"""
struct S3DBMeta <: Metadata
    collection::String
    dataset::String
    store::S3DB
    timezone::TimeZone
end

"""
    FFSMeta

Metadata for a [`FFS`](@ref) dataset.
"""
struct FFSMeta <: Metadata
    collection::String
    dataset::String
    store::FFS
    column_order::Vector{String}
    column_types::Dict{String,DataType}
    timezone::TimeZone
end

"""
    get_metadata(coll::String, ds::String, store::S3DB)::S3DBMeta

Retrieves the metadata for a dataset from the [`S3DB`](@ref) store.
"""
function get_metadata(coll::String, ds::String, store::S3DB)::S3DBMeta
    tz = get_tz(coll, ds)
    return S3DBMeta(coll, ds, store, tz)
end

"""
    get_metadata(coll::String, ds::String, store::FFS)::FFSMeta

Retrieves the metadata for a dataset from the [`FFS`](@ref) store.
"""
function get_metadata(coll::String, ds::String, store::FFS)::FFSMeta
    s3_key = generate_s3_metadata_key(coll, ds, store)

    try
        file = @mock s3_cached_get(store.bucket, s3_key)
        data = JSON.parse(read(file, String))
        tz = TimeZone(data["timezone"])
        column_types = Dict(k => decode_type(v) for (k, v) in pairs(data["column_types"]))
        return FFSMeta(coll, ds, store, data["column_order"], column_types, tz)
    catch err
        if isa(err, AWSException) && err.code == "NoSuchKey"
            throw(MissingDataError(coll, ds))
        else
            throw(err)
        end
    end
end

"""
    write_metadata(metadata::FFSMeta)

Writes the metadata for a dataset to its [`FFS`](@ref) store.
"""
function write_metadata(metadata::FFSMeta)
    s3key = generate_s3_metadata_key(metadata.collection, metadata.dataset, metadata.store)
    data = Dict(
        "column_order" => metadata.column_order,
        "column_types" => Dict(k => encode_type(v) for (k, v) in metadata.column_types),
        "timezone" => metadata.timezone.name,
    )
    @mock s3_put(metadata.store.bucket, s3key, JSON.json(data))
    return nothing
end

"""
    decode_type(str::AbstractString)::DataType

Decodes the string representation of a DataType.
"""
function decode_type(str::AbstractString)::DataType
    try
        return haskey(CUSTOM_TYPES, str) ? CUSTOM_TYPES[str] : getfield(Base, Symbol(str))
    catch err
        throw(ErrorException("Unable to decode custom type '$str'."))
    end
end

"""
    encode_type(str::AbstractString)::DataType

Encodes a DataType as a String.
In some cases, a generic Abstract type is used instead of specific concrete types.
"""
function encode_type(data_type::DataType)::String
    return if data_type <: AbstractString
        "AbstractString"
    elseif data_type == Bool
        # note that Bool <: Integer
        "Bool"
    elseif data_type <: Integer
        "Integer"
    elseif data_type <: AbstractFloat
        "AbstractFloat"
    else
        repr(data_type)
    end
end

"""
    sanitize_type(data_type::DataType)::DataType

Encodes and then decodes a DataType. This may result in an abstract type of the original
input type. See [`encode_type`](@ref) for more info.
"""
function sanitize_type(data_type::DataType)::DataType
    return decode_type(encode_type(data_type))
end

"""
    unix2zdt(ts::Int, tz::TimeZone=tz"UTC")::ZonedDateTime

Converts a unix timestamp to a `ZonedDateTime`. Function calls are `@memoize`-ed.

## Example
```julia
julia> ts = 1577836800

julia> unix2zdt(ts)
2020-01-01T00:00:00+00:00

julia> unix2zdt(ts, tz"America/New_York")
2019-12-31T19:00:00-05:00
```
"""
@memoize function unix2zdt(ts::Int, tz::TimeZone=tz"UTC")::ZonedDateTime
    dt = unix2datetime(ts)
    return try
        ZonedDateTime(dt, tz; from_utc=true)
    catch err
        (isa(tz, VariableTimeZone) && dt >= tz.cutoff) || throw(err)
        # By default, TimeZones.jl only supports a DateTime that can be represented by an Int32
        # https://juliatime.github.io/TimeZones.jl/stable/faq/#future_tzs-1
        warn(LOGGER, "Unable to localize '$dt' (UTC) as '$tz', falling back to 'UTC'.")
        ZonedDateTime(dt, tz"UTC"; from_utc=true)
    end
end

"""
    utc_day_floor(dt::ZonedDateTime)::ZonedDateTime

Converts a `ZonedDateTime` to UTC and day-floors it. Function calls are `@memoize`-ed.
"""
@memoize function utc_day_floor(dt::ZonedDateTime)::ZonedDateTime
    return floor(astimezone(dt, tz"UTC"), Dates.Day)
end

"""
    get_s3_file_timestamp(s3_key::String)::Int

Extracts the unix timestamp from `S3DB` and `FFS` file s3 keys.
"""
function get_s3_file_timestamp(s3_key::String)::Int
    file_name = rsplit(s3_key, "/"; limit=2)[end]
    return parse(Int, split(file_name, "."; limit=2)[1])
end

"""
    generate_s3_file_key(
        collection::AbstractString, dataset::AbstractString, dt::ZonedDateTime, store::S3Store
    )::String

Generates the s3 key for a `S3DB` and/or `FFS` file.
"""
function generate_s3_file_key(
    collection::AbstractString, dataset::AbstractString, dt::ZonedDateTime, store::S3Store
)::String
    day = utc_day_floor(dt)
    year = Dates.year(day)
    file_name = "$(zdt2unix(Int, day)).csv.gz"
    return joinpath(store.prefix, collection, dataset, "year=$(year)", file_name)
end

"""
    generate_s3_metadata_key(
        collection::AbstractString, dataset::AbstractString, store::S3Store
    )::String

Generates the s3 key for a `S3DB` and/or `FFS` dataset metadata file.
"""
function generate_s3_metadata_key(
    collection::AbstractString, dataset::AbstractString, store::S3Store
)::String
    return joinpath(store.prefix, collection, dataset, "METADATA.json")
end
