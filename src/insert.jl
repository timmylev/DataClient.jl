using TimeZones: zdt2unix

"""
    insert(
        collection::AbstractString,
        dataset::AbstractString,
        dataframe::DataFrame,
        store_id::AbstractString;
        details::Union{Nothing,Dict{String,String}}=nothing,
        column_types::Union{Nothing,Dict}=nothing,
    )

Inserts a `DataFrame` into a new or existing dataset in the specified store.

The insert operation is only supported for [`FFS`](@ref)-type stores. The input
`DataFrame` must contain a `target_start` column of type `ZonedDateTime`. This column
is used as the index when querying for data using the [`gather`](@ref) function.

If inserting data into an existing dataset, the input `DataFrame` will be merged and
deduplicated with any pre-existing data within the dataset. The process will fail if the
input `DataFrame` has any missing columns or incompatible column types.

# Arguments
- `collection`: The name of the dataset's collection
- `dataset`: The name of the dataset
- `dataframe`: The input `DataFrame` that is to be stored
- `store_id`: The backend store id

# Keywords
- `details`: (Optional) Details about the dataset that will be stored in the backend.
    This is solely for informational purposes, it serves no functional purpose.
    Specifying this when inserting data into an existing dataset will merge in any new
    or updated details.
- `column_types`: (Optional) When inserting a `DataFrame` into a new dataset, a type map
    for the dataset columns is generated and stored in the dataset metadata. This type
    map will then be used to validate future insertions of new data into the dataset.
    The automatically generated type map is typically based off AbstractTypes of the
    input DataFrame's column types, refer to [`sanitize_type`](@ref) for more details.
    Sometimes, the user may want to explicitly lock the column to a specific concrete
    type, or, allow missing values (using Union{Missing,T}). This keyword supports just
    that by overwriting the default generated type for the column. Attempting to modify
    the type map of an existing dataset is not supported and will result in an error.

## Example for Specifying Column Types
```julia
julia> new_dataframe = DataFrame(
    target_start=[
        ZonedDateTime(2020, 1, 1, 1, tz"America/New_York"),
        ZonedDateTime(2020, 1, 1, 2, tz"America/New_York"),
    ],
    string_col=["a", "b"],
    int_col=[11, 13],
)

# By default, the automatically generated type map will be:
# Dict(
#     "target_start" => ZonedDateTime,
#     "string_col" => AbstractString,
#     "int_col" => Integer,
# )

# We can modify these by specifying user-defined types to allow more or less
# flexibility for future insertion of new data into the dataset. For example,
# the following locks 'int_col' to `Int64` only and allows `missing`s in 'string_col'
julia> col_types = Dict(
    "string_col" => Union{Missing,AbstractString},
    "int_col" => Int64,
)

julia> insert(
    "new-collection",
    "new-dataset",
    new_dataframe,
    "my-store-id";
    details=Dict("Description"=>"My insert demo."),
    column_types=col_types,
)

# Note that modifying the stored type map of an existing dataset is not supported.
# So, this can only be done when inserting data to a new dataset for the first time.
```
"""
function insert(
    collection::AbstractString,
    dataset::AbstractString,
    dataframe::DataFrame,
    store_id::AbstractString;
    details::Union{Nothing,Dict{String,String}}=nothing,
    column_types::Union{Nothing,Dict}=nothing,
)
    store = get_backend(store_id)
    c_types = isnothing(column_types) ? nothing : convert(ColumnTypes, column_types)
    return _insert(
        collection, dataset, dataframe, store; details=details, column_types=c_types
    )
end

function _insert(
    collection::AbstractString,
    dataset::AbstractString,
    dataframe::DataFrame,
    store::FFS;
    details::Union{Nothing,Dict{String,String}}=nothing,
    column_types::Union{Nothing,ColumnTypes}=nothing,
)
    _validate(dataframe, store)
    metadata = @mock _ensure_created(
        collection, dataset, dataframe, store; details=details, column_types=column_types
    )

    @memoize groupkey(zdt) = zdt2unix(Int, utc_day_floor(zdt))
    # add a group key column
    dataframe.group_key = map(groupkey, dataframe.target_start)
    grouped = groupby(dataframe, :group_key)

    for (gk, sf) in pairs(grouped)
        s3key = generate_s3_file_key(collection, dataset, unix2zdt(gk.group_key), store)
        @mock _merge(sf, s3key, metadata)
    end

    # remove the group key column
    select!(dataframe, Not(:group_key))
    return nothing
end

"""
    _merge(dataframe::AbstractDataFrame, s3key::AbstractString, metadata::FFSMeta)

Merges a DataFrame into a new or existing s3 file. The merged data will be sorted and
deduplicated before writing to S3.
"""
function _merge(dataframe::AbstractDataFrame, s3key::AbstractString, metadata::FFSMeta)
    # do not modify the original dataframe
    df = copy(dataframe)

    # encode ZonedDateTimes as unix timestamps
    for (col, type) in pairs(metadata.column_types)
        if type == ZonedDateTime
            df[!, col] = zdt2unix.(Int, df[!, col])
        end
    end

    # check if there is existing data in S3
    existing = try
        obj = @mock s3_get(metadata.store.bucket, s3key)
        existing = DataFrame(CSV.File(obj))
    catch err
        isa(err, AWSException) && err.code == "NoSuchKey" || throw(err)
        DataFrame()
    end

    len_old = nrow(existing)

    merged = reduce(vcat, (existing, df); cols=metadata.column_order)
    sort!(unique!(merged), metadata.column_order)

    debug(
        LOGGER,
        "Merged $(nrow(df)) new rows into $len_old old rows to get $(nrow(merged)) " *
        "total rows for '$s3key'.",
    )

    stream = IOBuffer()
    CSV.write(stream, merged; compress=true)
    # the stream is closed after compression, so use stream.data to access data
    @mock s3_put(metadata.store.bucket, s3key, stream.data)

    return nothing
end

"""
    _validate(dataframe::DataFrame, ::FFS)

Runs validations on the input DataFrame to ensure that it satisfies the basic dataset
requirements.
"""
function _validate(dataframe::DataFrame, ::FFS)
    if isempty(dataframe)
        throw(DataFrameError("Dataframe must not be empty."))

    elseif !("target_start" in names(dataframe))
        throw(DataFrameError("Missing required column `target_start` for insert."))

    elseif eltype(dataframe.target_start) != ZonedDateTime
        tp = eltype(dataframe.target_start)
        throw(DataFrameError("Column `target_start` must be a ZonedDateTime, found $tp."))
    end
end

"""
    _ensure_created(
        collection::AbstractString,
        dataset::AbstractString,
        dataframe::DataFrame,
        store::FFS;
        details::Union{Nothing,Dict{String,String}}=nothing,
        column_types::Union{Nothing,ColumnTypes}=nothing,
    )::FFSMeta

If running on a new dataset, creates dataset metadata based off the input DataFrame and
user-specified arguments. If running on an existing dataset, validates the input
DataFrame based off the exinsting dataset metadata.
"""
function _ensure_created(
    collection::AbstractString,
    dataset::AbstractString,
    dataframe::DataFrame,
    store::FFS;
    details::Union{Nothing,Dict{String,String}}=nothing,
    column_types::Union{Nothing,ColumnTypes}=nothing,
)::FFSMeta
    metadata = try
        @mock get_metadata(collection, dataset, store)
    catch err
        isa(err, MissingDataError) || throw(err)
        nothing
    end

    to_write = nothing

    if !isnothing(metadata)
        _validate_columns(dataframe, metadata)

        if !isnothing(column_types)
            warn(LOGGER, "Ignoring param `column_types`, dataset is already created.")
        end

        # Check if the existing dataset's metadata needs updating:
        # - update the metadata details if new details are specified
        # - update the last_modified only if >1 Day has passed
        current = something(metadata.details, Dict{String,String}())
        new = something(details, Dict{String,String}())
        updated_details = merge(current, new)
        updated_details = isempty(updated_details) ? nothing : updated_details

        elapsed = now(tz"UTC") - metadata.last_modified

        if elapsed > Day(1) || metadata.details != updated_details
            debug(LOGGER, "Updating existing metadata for dataset '$collection-$dataset'.")
            to_write = FFSMeta(;
                collection=metadata.collection,
                dataset=metadata.dataset,
                store=metadata.store,
                column_order=metadata.column_order,
                column_types=metadata.column_types,
                timezone=metadata.timezone,
                last_modified=now(tz"UTC"),  # update
                details=updated_details,  # update
            )
        end

    else
        ds = "'$collection-$dataset'"
        debug(LOGGER, "Metadata for $ds does not exist, creating metadata...")

        col_order = names(dataframe)
        col_types::ColumnTypes = Dict(
            names(dataframe) .=> sanitize_type.(eltype.(eachcol(dataframe)))
        )

        # update the column types if the user specified any customs types
        if !isnothing(column_types)
            for (col_name, new_type) in pairs(column_types)
                if col_name in col_order
                    col_types[col_name] = new_type
                else
                    warn(
                        LOGGER,
                        "The column '$col_name' in the user-defined `column_types` " *
                        "is not present in the input DataFrame, ignoring it...",
                    )
                end
            end
        end

        df_tz = dataframe.target_start[1].timezone

        to_write = FFSMeta(;
            collection=collection,
            dataset=dataset,
            store=store,
            column_order=col_order,
            column_types=col_types,
            timezone=df_tz,
            last_modified=now(tz"UTC"),
            details=details,
        )

        # In case we're using user-defined columns and types, run a validation to ensure
        # that it is compatible with the input dataframe.
        _validate_columns(dataframe, to_write; user_defined=true)
    end

    if !isnothing(to_write)
        @mock write_metadata(to_write)
    end

    return something(to_write, metadata)
end

"""
    _validate_columns(dataframe::DataFrame, metadata::FFSMeta; user_defined::Bool=false)

Validates a DataFrame's columns and types against the provided metadata.
"""
function _validate_columns(
    dataframe::DataFrame, metadata::FFSMeta; user_defined::Bool=false
)
    ds = "'$(metadata.collection)-$(metadata.dataset)'"

    # ensure that all required cols are present, and warn if there are extra cols
    cols_required = metadata.column_order
    cols_available = names(dataframe)

    cols_missing = [col for col in cols_required if !(col in cols_available)]
    cols_extra = [col for col in cols_available if !(col in cols_required)]

    if !isempty(cols_missing)
        throw(DataFrameError("Missing required columns $cols_missing for dataset $ds."))
    elseif !isempty(cols_extra)
        warn(
            LOGGER,
            "Extra columns $cols_extra found in the input DataFrame for dataset $ds " *
            "will be ignored.",
        )
    end

    # ensure that col types are compatible
    df_col_types = Dict(names(dataframe) .=> eltype.(eachcol(dataframe)))
    keyword = user_defined ? "user-defined" : "stored"
    for (key, required_type) in pairs(metadata.column_types)
        df_col_type = df_col_types[key]
        # When storing a dataset metadata for the first time, the column types are
        # typically converted into Abstract types, see sanitize_type() in src/common.jl
        # for more info. So, the `required_type` here is typically a Abstract type,
        # unless the user has specified otherwise when creating the dataset.
        if !(df_col_type <: required_type)
            throw(
                DataFrameError(
                    "The input DataFrame column '$key' has type '$df_col_type' which " *
                    "is incompatible with the $keyword type of '$required_type'",
                ),
            )
        end
    end
end
