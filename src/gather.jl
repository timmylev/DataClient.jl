using .AWSUtils: s3_cached_get
using AWS.AWSExceptions: AWSException
using TimeZones: zdt2unix

const _GATHER_ASYNC_NTASKS = 8

"""
    gather(
        collection::AbstractString,
        dataset::AbstractString,
        start_dt::ZonedDateTime,
        end_dt::ZonedDateTime,
        [store_id::AbstractString,]
    )::DataFrame

Gathers data from a target dataset as a `DataFrame`.

# Arguments
- `collection`: The name of the dataset's collection
- `dataset`: The name of the dataset
- `start_dt`: The start bound (inclusive) of the `Index` column .
- `end_dt`: The end bound (inclusive) for the `Index` column.
- `store_id`: (Optional) The backend store id.

!!! note "IMPORTANT"
    The `start_dt` and `end_dt` filters are only applied to the `Index` column of the
    dataset. For `S3DB`(@ref) datsets, this is always the `target_start` column. For
    `FFS`(@ref) datasets, it will depend on which column was set as the index when the
    dataset was first created. Any "target_end" column (if available) is irrelevant.

!!! note
    It is recommended to specify a `store_id` for efficiency reasons. If none is provided,
    each available store will be iteratively checked in order of precedence until the first
    store containing the target dataset is found. Refer to [Configs and Backend](@ref) for
    more info about store precedence.
"""
function gather(
    collection::AbstractString,
    dataset::AbstractString,
    start::ZonedDateTime,
    stop::ZonedDateTime,
)::DataFrame
    # get_backend() returns an OrderedDict, i.e. the search order in configs.yaml
    for (name, store) in pairs(get_backend())
        data = nothing

        try
            data = _gather(collection, dataset, start, stop, store)
        catch err
            isa(err, MissingDataError) || throw(err)
        end

        if !isnothing(data) && !isempty(data)
            return data
        end
    end

    throw(MissingDataError(collection, dataset, start, stop))
end

function gather(
    collection::AbstractString,
    dataset::AbstractString,
    start_dt::ZonedDateTime,
    end_dt::ZonedDateTime,
    store_id::AbstractString,
)::DataFrame
    data = _gather(collection, dataset, start_dt, end_dt, get_backend(store_id))

    return if !isempty(data)
        data
    else
        throw(MissingDataError(collection, dataset, start_dt, end_dt))
    end
end

function _gather(
    collection::AbstractString, dataset::AbstractString, start::T, stop::T, store::S3Store
)::DataFrame where {T}
    meta = @mock get_metadata(collection, dataset, store)
    ds_name = "'$(meta.collection)-$(meta.dataset)'"

    keys = gen_s3_file_keys(start, stop, meta)
    nkeys = length(keys)
    debug(LOGGER, "Generated $nkeys file keys for $ds_name")

    # If grabbing many files, do a s3-list first to filter out missing files.
    if length(keys) > _GATHER_ASYNC_NTASKS
        t1 = @elapsed keys = @mock _filter_missing(keys, meta)
        nkeys = length(keys)
        rtime = "($(s_fmt(t1)))"
        debug(LOGGER, "Listing $nkeys file keys from $ds_name took $rtime")
    end

    t2 = @elapsed results = @mock _load_s3_files(keys, start, stop, meta)
    rtime = "($(s_fmt(t2)))"
    debug(LOGGER, "Loading and merging $nkeys files from $ds_name took $rtime")

    return results
end

function _filter_missing(s3_keys::Vector{String}, meta::S3Meta)::Vector{String}
    get_s3_dir(s3_key) = "$(rsplit(s3_key, "/"; limit=2)[1])/"

    s3_dirs = Set([get_s3_dir(k) for k in s3_keys])

    keys_all = Set(s3_keys)
    keys_found = Set{String}()

    # list all files in the yearly directories and filter for matches.
    for dir in s3_dirs
        count = 0
        match = 0

        for key in @mock s3_list_keys(meta.store.bucket, dir)
            if key in keys_all
                match += 1
                push!(keys_found, key)
            end
            count += 1
        end

        trace(LOGGER, "Listed $count keys with $match matches in s3 prefix: $dir")
    end

    keys_missing = setdiff(keys_all, keys_found)
    keys_found = sort(collect(keys_found))
    keys_missing = sort(collect(keys_missing))

    ratio = "$(length(keys_found))/$(length(keys_all))"
    msg = "Found $ratio files for $(meta.collection)-$(meta.dataset)"

    if !isempty(keys_found)
        msg *= "\nFiles Found:\n - $(join(keys_found, "\n - "))"
    end

    if !isempty(keys_missing)
        msg *= "\nFiles Missing:\n - $(join(keys_missing, "\n - "))"
    end

    trace(LOGGER, msg)

    return keys_found
end

function _load_s3_files(
    file_keys::Vector{String}, start::T, stop::T, meta::S3Meta
)::DataFrame where {T}
    to = TimerOutput()
    dfs = Dict{String,DataFrame}()

    storage_format = get_storage_format(meta)

    @timeit to "async loop" begin
        asyncmap(file_keys; ntasks=_GATHER_ASYNC_NTASKS) do key
            file_path = nothing

            try
                file_path = @mock s3_cached_get(meta.store.bucket, key)

            catch err
                isa(err, AWSException) && err.code == "NoSuchKey" || throw(err)
                debug(LOGGER, "S3 object '$key' not found.")

            finally
                if !isnothing(file_path)
                    @timeit to "df load" df = load_df(storage_format, file_path)

                    @timeit to "df filter" filter_df!(df, start, stop, meta; s3_key=key)

                    @timeit to "df xform" _process_dataframe!(df, meta)

                    dfs[key] = df
                end
            end
        end
    end

    @timeit to "vcat dfs" results = if !isempty(dfs)
        vcat([dfs[key] for key in sort(collect(keys(dfs)))]...)
    else
        DataFrame()
    end

    trace(LOGGER) do
        # Adds the remaining time in the async block, i.e. the idle time waiting for
        # s3 downloads and not doing anything else
        TimerOutputs.complement!(to)
        temp = IOBuffer()
        print_timer(temp, to; sortby=:firstexec)
        "Timing for _load_s3_files():\n" * String(take!(temp))
    end

    return results
end

function _process_dataframe!(df::DataFrame, metadata::S3DBMeta)
    # reorder the df columns and sort the rows
    order_cols = ["target_start", "target_end", "target_bounds", "release_date", "tag"]
    other_cols = [c for c in names(df) if !(c in order_cols)]
    col_order = vcat(order_cols[1:4], other_cols, order_cols[end])
    select!(df, col_order)
    sort!(df, col_order)

    # convert unix datetimes to zdt
    zdt_cols = ["target_start", "target_end", "release_date"]
    for col in zdt_cols
        df[!, col] = unix2zdt.(df[!, col], metadata.timezone)
    end

    # decode 'list' types
    parser(el) = ismissing(el) ? el : identity.(JSON.parse(el; null=missing))
    for (col, type) in pairs(metadata.meta["type_map"])
        if type == "list"
            df[!, col] = parser.(df[!, col])

        elseif type == "bool"
            df[!, col] = convert.(Bool, df[!, col])
        end
    end

    # convert Int bounds to intervals notation, eg. "[)"
    df.target_bounds = map(b -> DataClient.BOUNDS[b], df.target_bounds)

    return nothing
end

function _process_dataframe!(df::DataFrame, metadata::FFSMeta)
    for (col_name, col_type) in metadata.column_types
        if col_type == ZonedDateTime
            df[!, col_name] = unix2zdt.(df[!, col_name], metadata.timezone)
        elseif !(eltype(df[!, col_name]) <: col_type)
            # The CSV.File() function that we use to load in csv data automatically
            # detects column types, check the detected type just in-case.
            throw(
                DataFrameError(
                    "Loaded type '$(eltype(df[!, col_name]))' does not match registered" *
                    " type '$col_type' for column '$col_name', please investigate.",
                ),
            )
        end
    end
    return nothing
end
