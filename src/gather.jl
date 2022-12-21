using .AWSUtils: s3_cached_get
using AWS.AWSExceptions: AWSException
using TimeZones: zdt2unix

const _GATHER_ASYNC_NTASKS = 8

const _S3DB_RELEASE_COL = :release_date
const _S3DB_ZDT_COLS = (:target_start, :target_end, _S3DB_RELEASE_COL)
# do not include these cols when grouping rows to find the latest release
const _S3DB_NON_ID_COLS = [_S3DB_RELEASE_COL, :tag]

"""
    gather(
        collection::AbstractString,
        dataset::AbstractString,
        start_dt::ZonedDateTime,
        end_dt::ZonedDateTime,
        [store_id::AbstractString,];
        sim_now::Union{ZonedDateTime,Nothing}=nothing,
        filters::Union{Nothing,Dict{Symbol,Vector{P}}}=nothing,
        filters_in::Bool=true,
    )::DataFrame where {P}

Gathers data from a target dataset as a `DataFrame`.

# Arguments
- `collection`: The name of the dataset's collection
- `dataset`: The name of the dataset
- `start_dt`: The start bound (inclusive) of the `Index` column .
- `end_dt`: The end bound (inclusive) for the `Index` column.
- `store_id`: (Optional) The backend store id. It is recommended to specify a `store_id`
    for efficiency reasons. If none is provided, each available store will be iteratively
    checked in order of precedence until the first store containing the target dataset
    is found. Refer to [Configs and Backend](@ref) for more info about store precedence.

# Keywords
- `sim_now`: (Optional) When supplied, only the row with the latest `release_date` up to
    the `sim_now` (cutoff) will be returned for every group of rows with the same id. The
    id of a row is the primary key of the row minus the `release_date` and `tag`. This
    is only supported for [`S3DB`](@ref) stores.
- `filters`: (Optional) Additional column-wise containment filters to apply to the gather
    query. This filter is run before the `sim_now` filter if both are provided, which may
    boost overall query performance when compared to just using `sim_now` alone.
- `filters_in`: (Optional) Determines whether to filter-IN or to filter-OUT rows with
    column values that match the supplied `filters`. This is only relevant when the
    `filters` kwarg is supplied and it defaults to `true`.

!!! note "IMPORTANT"
    The `start_dt` and `end_dt` filters are only applied to the `Index` column of the
    dataset. For [`S3DB`](@ref) datsets, this is always the `target_start` column. For
    [`FFS`](@ref) datasets, it will depend on which column was set as the index when the
    dataset was first created. Any "target_end" column (if available) is irrelevant.

## Cache Configs
When retrieving data from a sub-type of `S3Store`, a file cache is automatically
instantiated for each Julia session to cache downloaded S3 files. By default, this cache
is ephemeral and will be deleted at the end of the session. Also by default, downloaded
S3 files that are compressed will be decompressed before caching, though this is only
suported for a limited number of compression types. The following config file setting
([Configs and Backend](@ref)) and/or environment variables can be supplied to modify the
default cache behaviour. Refer to [`AWSUtils.s3_cached_get`](@ref) and
[`AWSUtils.FileCache`](@ref) for more details:
- `DATACLIENT_CACHE_DIR` (`String`): The absolute path to a custom directory to be used
    as the cache. Files cached here will be persistent, i.e. not removed at the end of
    the session.
- `DATACLIENT_CACHE_SIZE_MB` (`Int`): The max cache size in MB before files are
    de-registered/removed from the LRU during the session in real-time.
- `DATACLIENT_CACHE_EXPIRE_AFTER_DAYS` (`Int`): This is only relevant when initializing
    the cache at the start of the session if a custom cache dir is specified. Files in
    the cache dir that is older than this period will be removed during initialisation.
    The default is 90 days.
- `DATACLIENT_CACHE_DECOMPRESS` (`Bool`): Whether or not to decompress S3 files before
    caching.

!!! note "WARNING: Multi-process Shared Cache"
    The [`AWSUtils.FileCache`](@ref) (LRU-based) is thread-safe, but not multi-process safe.
    When sharing the same (custom) cache directory across multiple processes, there may
    be cases where one proccess starts removing file(s) from the cache dir due to it
    hitting the max cache size limit. This is a problem for the other processes because
    the removed file(s) are still registered in their cache registries even though the
    underlying files no longer exists. To reduce the likelyhood of this happening, set
    the max cache size limit to a high number such that file removals do not happen.
    Alternatively, do not use a custom cache dir for multi-process use cases and stick
    with the default ephemeral cache instead, which creates a separate cache dir for
    each process.
"""
function gather(
    collection::AbstractString,
    dataset::AbstractString,
    start::ZonedDateTime,
    stop::ZonedDateTime;
    sim_now::Union{ZonedDateTime,Nothing}=nothing,
    filters::Union{Nothing,Dict{Symbol,Vector{P}}}=nothing,
    filters_in::Bool=true,
)::DataFrame where {P}
    # get_backend() returns an OrderedDict, i.e. the search order in configs.yaml
    for (name, store) in pairs(get_backend())
        data = nothing

        try
            data = _gather(
                collection,
                dataset,
                start,
                stop,
                store;
                sim_now=sim_now,
                filters=filters,
                filters_in=filters_in,
            )
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
    store_id::AbstractString;
    sim_now::Union{ZonedDateTime,Nothing}=nothing,
    filters::Union{Nothing,Dict{Symbol,Vector{P}}}=nothing,
    filters_in::Bool=true,
)::DataFrame where {P}
    store = get_backend(store_id)
    data = _gather(
        collection,
        dataset,
        start_dt,
        end_dt,
        store;
        sim_now=sim_now,
        filters=filters,
        filters_in=filters_in,
    )

    return if !isempty(data)
        data
    else
        throw(MissingDataError(collection, dataset, start_dt, end_dt))
    end
end

function _gather(
    collection::AbstractString,
    dataset::AbstractString,
    start::T,
    stop::T,
    store::S3Store;
    sim_now::Union{ZonedDateTime,Nothing}=nothing,
    filters::Union{Nothing,Dict{Symbol,Vector{P}}}=nothing,
    filters_in::Bool=true,
)::DataFrame where {T,P}
    if !isnothing(sim_now) && !isa(store, S3DB)
        throw(ArgumentError("The `sim_now` arg is only supported for `S3DB` stores."))
    end

    meta = @mock get_metadata(collection, dataset, store)
    ds_name = "'$(meta.collection)-$(meta.dataset)'"

    keys = gen_s3_file_keys(start, stop, meta)
    nkeys = length(keys)
    trace(LOGGER, "Generated $nkeys file keys for $ds_name")

    # If grabbing many files, do a s3-list first to filter out missing files.
    if length(keys) > _GATHER_ASYNC_NTASKS
        t1 = @elapsed keys = @mock _filter_missing(keys, meta)
        nkeys = length(keys)
        rtime = "($(s_fmt(t1)))"
        trace(LOGGER, "Listing $nkeys file keys from $ds_name took $rtime")
    end

    t2 = @elapsed results = @mock _load_s3_files(
        keys, start, stop, meta; sim_now=sim_now, filters=filters, filters_in=filters_in
    )
    rtime = "($(s_fmt(t2)))"
    debug(LOGGER, "Loading $nkeys files from $ds_name took $rtime")

    # Attach metadata to the dataframe, see `DataFrame.metadata!`
    metadata!(results, "metadata", meta; style=:note)

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
    file_keys::Vector{String},
    start::T,
    stop::T,
    meta::S3Meta;
    sim_now::Union{ZonedDateTime,Nothing}=nothing,
    filters::Union{Nothing,Dict{Symbol,Vector{P}}}=nothing,
    filters_in::Bool=true,
)::DataFrame where {T,P}
    to = TimerOutput()
    dfs = Dict{String,AbstractDataFrame}()

    file_format = get_file_format(meta)
    to_decompress = get(Configs.get_configs(), "DATACLIENT_CACHE_DECOMPRESS", true)

    # additional 'containment' filters
    df_filter_plus = isnothing(filters) ? nothing : df_filter_factory(filters, filters_in)

    @timeit to "async loop" begin
        asyncmap(file_keys; ntasks=_GATHER_ASYNC_NTASKS) do key
            file_path = nothing

            try
                file_path = @mock s3_cached_get(
                    meta.store.bucket, key; decompress=to_decompress
                )

            catch err
                isa(err, AWSException) && err.code == "NoSuchKey" || throw(err)
                debug(LOGGER, "S3 object '$key' not found.")

            finally
                if !isnothing(file_path)
                    # the file may have been decompressed beforehand by s3_cached_get
                    _, compression = FileFormats.detect_format(file_path)

                    @timeit to "df load" df = load_df(file_path, file_format, compression)

                    # The index filter. Typically, this will only run on the first and last file
                    @timeit to "df filter" df = filter_df(df, start, stop, meta; s3_key=key)

                    # additional 'containment' filters
                    if !isempty(df) && !isnothing(df_filter_plus)
                        @timeit to "df filter_plus" df = filter(df_filter_plus, df)
                    end

                    # sim_now filter
                    if !isempty(df) && !isnothing(sim_now)
                        @timeit to "df sim_now" df = _filter_sim_now(df, meta, sim_now)
                    end

                    if !isempty(df)
                        dfs[key] = df
                    end
                end
            end
        end
    end

    results = if !isempty(dfs)
        @timeit to "vcat dfs" df = vcat([dfs[key] for key in sort(collect(keys(dfs)))]...)
        @timeit to "df xform" _process_dataframe!(df, meta)
        df
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
    # a very large proportion of zdts are identical
    cache = Dict{Int,ZonedDateTime}()
    cached_unix2zdt(ts::Int)::ZonedDateTime =
        get!(cache, ts) do
            unix2zdt(ts, metadata.timezone)
        end

    # convert unix datetimes to zdt
    for col in _S3DB_ZDT_COLS
        df[!, col] = map(cached_unix2zdt, df[!, col])
    end

    # decode 'list' types
    parse_list(el) = ismissing(el) ? el : identity.(JSON.parse(el; null=missing))
    parse_bool(el) = ismissing(el) ? el : convert(Bool, el)

    for (col, type) in pairs(metadata.meta["type_map"])
        if type == "list"
            df[!, col] = parse_list.(df[!, col])

        elseif type == "bool"
            df[!, col] = parse_bool.(df[!, col])
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

function _filter_sim_now(df::DataFrame, metadata::S3DBMeta, sim_now::ZonedDateTime)
    sim_now_epoch = zdt2unix(Int, sim_now)
    s3db_pkeys = Symbol.(metadata.meta["superkey"])
    unique_keys = setdiff(s3db_pkeys, _S3DB_NON_ID_COLS)

    selections = Vector{Int}()
    sizehint!(selections, nrow(df))

    # - DataFrame.groupby is very efficient, so use it.
    # - DataFrames.combine is not ideal for our usecase, so use a custom impementation.
    for sdf in groupby(df, unique_keys)
        latest = 0
        selected = 0
        for row in eachrow(sdf)
            rd = row[_S3DB_RELEASE_COL]
            if rd <= sim_now_epoch && rd > latest
                latest = rd
                selected = first(parentindices(row))
            end
        end
        if selected != 0
            push!(selections, selected)
        end
    end

    # There's no point in materializing the selections at this stage because
    # we'll be running a vcat in the next step
    return @view df[selections, :]
end

# generates a DF filter function given filters as a Dict
function df_filter_factory(filters::Dict{Symbol,Vector{T}}, filters_in::Bool) where {T}
    fvec = collect(filters)
    fkeys = first.(fvec)
    fvals = Set.(last.(fvec))  # convert to set
    results = Vector{Bool}(undef, length(fkeys))  # pre-allocate results vector
    function fn(args...)
        for (i, el) in enumerate(args)
            results[i] = (el in fvals[i]) == filters_in
        end
        return all(results)
    end
    return fkeys => fn
end
