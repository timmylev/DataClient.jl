using TimeZones: zdt2unix

"""
    insert(
        collection::AbstractString,
        dataset::AbstractString,
        dataframe::DataFrame,
        store_id::AbstractString;
        details::Union{Nothing,Dict{String,String}}=nothing,
    )

Inserts a `DataFrame` to a new or existing dataset in the specified store.

The insert operation is only supported for [`FFS`](@ref)-type stores. The input
`DataFrame` must contain a `target_start` column of type `ZonedDateTime`.

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
"""
function insert(
    collection::AbstractString,
    dataset::AbstractString,
    dataframe::DataFrame,
    store_id::AbstractString;
    details::Union{Nothing,Dict{String,String}}=nothing,
)
    store = get_backend(store_id)
    return _insert(collection, dataset, dataframe, store, details)
end

function _insert(
    collection::AbstractString,
    dataset::AbstractString,
    dataframe::DataFrame,
    store::FFS,
    details::Union{Nothing,Dict{String,String}},
)
    _validate(dataframe, store)
    metadata = @mock _ensure_created(collection, dataset, dataframe, store, details)

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

function _ensure_created(
    collection::AbstractString,
    dataset::AbstractString,
    dataframe::DataFrame,
    store::FFS,
    details::Union{Nothing,Dict{String,String}},
)::FFSMeta
    metadata = try
        @mock get_metadata(collection, dataset, store)
    catch err
        isa(err, MissingDataError) || throw(err)
        nothing
    end

    ds = "'$collection-$dataset'"

    to_write = if !isnothing(metadata)
        debug(LOGGER, "Updating existing metadata for dataset $ds.")

        # ensure that all required cols are present, and warn if there are extra cols
        cols_required = metadata.column_order
        cols_available = names(dataframe)

        cols_missing = [col for col in cols_required if !(col in cols_available)]
        cols_extra = [col for col in cols_available if !(col in cols_required)]

        if !isempty(cols_missing)
            throw(
                DataFrameError(
                    "Missing required columns $cols_missing for existing dataset $ds."
                ),
            )
        elseif !isempty(cols_extra)
            warn(
                LOGGER,
                "Extra columns $cols_extra found in the input DataFrame for existing " *
                "dataset $ds will be ignored.",
            )
        end

        # ensure that col types are compatible
        col_types = Dict(names(dataframe) .=> eltype.(eachcol(dataframe)))
        for (key, stored_type) in pairs(metadata.column_types)
            col_type = col_types[key]
            # When storing a dataset metadata for the first time, the column types are
            # typically converted into Abstract types, see encode_type() in src/common.jl
            # for more info. So, the `stored_type` here is typically a Abstract type.
            if !(col_type <: stored_type)
                throw(
                    DataFrameError(
                        "The input DataFrame column '$key' has type '$col_type' which " *
                        "is incompatible with the existing stored type of '$stored_type'",
                    ),
                )
            end
        end

        # update the datasets details if applicable
        new_details = if isnothing(metadata.details)
            details
        elseif !isnothing(details)
            merge!(metadata.details, details)
        else
            metadata.details
        end

        FFSMeta(;
            collection=metadata.collection,
            dataset=metadata.dataset,
            store=metadata.store,
            column_order=metadata.column_order,
            column_types=metadata.column_types,
            timezone=metadata.timezone,
            last_modified=now(tz"UTC"),  # update
            details=new_details,
        )

    else
        debug(LOGGER, "Metadata for $ds does not exist, creating metadata...")

        column_order = names(dataframe)
        column_types = Dict(
            names(dataframe) .=> sanitize_type.(eltype.(eachcol(dataframe)))
        )
        tz = dataframe.target_start[1].timezone

        FFSMeta(;
            collection=collection,
            dataset=dataset,
            store=store,
            column_order=column_order,
            column_types=column_types,
            timezone=tz,
            last_modified=now(tz"UTC"),
            details=details,
        )
    end

    @mock write_metadata(to_write)

    return to_write
end
