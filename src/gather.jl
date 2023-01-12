using Base.Threads: @spawn, threadid

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
        excludes::Union{Nothing,Dict{Symbol,Vector{Q}}}=nothing,
        ntasks::Int=_GATHER_ASYNC_NTASKS,
    )::DataFrame where {P}

Gathers data from a target dataset as a `DataFrame`.

# Arguments
- `collection`: The name of the dataset's collection
- `dataset`: The name of the dataset
- `start_dt`: The start bound (inclusive) of the `Index` column .
- `end_dt`: The end bound (inclusive) for the `Index` column.
- `store_id`: (Optional) A registered backend store id or a backend URI. It is recommended
    to specify this for efficiency reasons. If none is provided, each available store will
    be iteratively checked in order of precedence until the first store containing the
    target dataset is found. Refer to [Configs and Backend](@ref) for more info about
    store precedence and registering custom stores.

# Keywords
- `sim_now`: (Optional) When supplied, only the row with the latest `release_date` up to
    the `sim_now` (cutoff) will be returned for every group of rows with the same id. The
    id of a row is the primary key of the row minus the `release_date` and `tag`. This
    is only supported for [`S3DB`](@ref) stores.
- `filters`: (Optional) Additional column-wise containment filters to apply to the query
    to only INCLUDE rows with matching column values. This filter is run before the
    `sim_now` filter if both are provided, which may boost overall query performance
    when compared to just using the `sim_now` filter alone.
- `excludes`: (Optional) This works in a similar but opposite way to the `filters` kwarg,
    it EXCLUDES any rows with matching column values.
- `ntasks`: (Optional) The number of tasks to run concurrently when downloading and
    processing s3 files, defaults to $_GATHER_ASYNC_NTASKS. Each task is run using Threads.@spawn, so
    multi-threading will take effect if enabled. Setting this to 1 will disable asynchrony
    and provide finer-grain timing logs for different stages during the gather, which
    maybe handy for benchmarking and debugging (clearer error messages / stack trace).

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
    excludes::Union{Nothing,Dict{Symbol,Vector{Q}}}=nothing,
    ntasks::Int=_GATHER_ASYNC_NTASKS,
)::DataFrame where {P,Q}
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
                excludes=excludes,
                ntasks=ntasks,
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
    excludes::Union{Nothing,Dict{Symbol,Vector{Q}}}=nothing,
    ntasks::Int=_GATHER_ASYNC_NTASKS,
)::DataFrame where {P,Q}
    store = get_backend(store_id)
    data = _gather(
        collection,
        dataset,
        start_dt,
        end_dt,
        store;
        sim_now=sim_now,
        filters=filters,
        excludes=excludes,
        ntasks=ntasks,
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
    excludes::Union{Nothing,Dict{Symbol,Vector{Q}}}=nothing,
    ntasks::Int=_GATHER_ASYNC_NTASKS,
)::DataFrame where {T,P,Q}
    if !isnothing(sim_now) && !isa(store, S3DB)
        throw(ArgumentError("The `sim_now` arg is only supported for `S3DB` stores."))
    end

    # ensure conflicting filters don't overlap
    if !isnothing(filters) && !isnothing(excludes) && !isdisjoint(filters, excludes)
        throw(ArgumentError("The `filters` and `excludes` keys must not overlap"))
    end

    ntasks <= 0 && throw(ArgumentError("`ntasks` must be positive"))

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
        keys,
        start,
        stop,
        meta;
        sim_now=sim_now,
        filters=filters,
        excludes=excludes,
        ntasks=ntasks,
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
    excludes::Union{Nothing,Dict{Symbol,Vector{Q}}}=nothing,
    ntasks::Int=_GATHER_ASYNC_NTASKS,
)::DataFrame where {T,P,Q}
    to = TimerOutput()
    # This will be `nothing` if both `filters` and `excludes` are nothing/empty
    custom_filter = df_filter_factory(filters, excludes)

    # Note that TimerOutputs.jl doesn't work with concurrent processing, so detailed
    # timings will only be available for sequential processing.
    @timeit to "load s3 files" dfs = if ntasks == 1
        # technically, we can just stick to asyncmap (the else clause) with ntasks=1, but
        # that may obfuscate internal error in earlier versions of Julia. So, just use a
        # for loop such that we can set ntasks=1 and get better stack traces when debugging
        [_load_s3_file(key, start, stop, meta, sim_now, custom_filter, to) for key in file_keys]
    else
        # - asyncmap is used simply because it is a convenient way to managed a fixed number
        #   of concurrent worker tasks. All worker tasks will be on the same thread.
        # - @spawn is then called by each worker task, which creates and runs a new Task on any
        #   available thread (where multithreading comes in, if enabled in Julia).
        asyncmap(file_keys; ntasks=ntasks) do key
            fetch(@spawn _load_s3_file(key, start, stop, meta, sim_now, custom_filter, nothing))
        end
    end

    filter!(!isnothing, dfs)

    results = if !isempty(dfs)
        @timeit to "vcat dfs" df = vcat(dfs...)
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

function _load_s3_file(
    s3_key::AbstractString,
    start::T,
    stop::T,
    meta::S3Meta,
    sim_now::Union{ZonedDateTime,Nothing},
    custom_filter_func,
    timer::Union{TimerOutput,Nothing},
)::Union{AbstractDataFrame,Nothing} where {T}
    trace(LOGGER, "Processing file '$s3_key' on thread $(threadid())...")

    # simply use a dummy timer if one is not given
    to = timer
    if isnothing(to)
        to = TimerOutput()
        disable_timer!(to)
    end

    try
        to_decompress = get(Configs.get_configs(), "DATACLIENT_CACHE_DECOMPRESS", true)
        @timeit to "s3 file download" file_path = @mock s3_cached_get(
            meta.store.bucket, s3_key; decompress=to_decompress
        )

        file_format, compression = FileFormats.detect_format(file_path)
        @timeit to "s3 file to df" df = load_df(file_path, file_format, compression)

        # The index filter. Typically, this will only run on the first and last file
        @timeit to "df filter index" df = filter_df(df, start, stop, meta; s3_key=s3_key)

        # Additional filters if `filters` and/or `excludes` are specified
        sdf = if !isempty(df) && !isnothing(custom_filter_func)
            @timeit to "df filter other" custom_filter_func(df)  # returns a view
        else
            df
        end

        # sim_now filter
        @timeit to "df filter sim_now" if !isempty(sdf) && !isnothing(sim_now)
            # note that if sdf is a SubDataFrame, this returns the row number
            # in the original DataFrame.
            selections = _filter_sim_now(sdf, meta, sim_now)
            # Always extract the selections as a view from the original dataframe
            sdf = @view df[selections, :]
        end

        return sdf

    catch err
        isa(err, AWSException) && err.code == "NoSuchKey" || throw(err)
        debug(LOGGER, "S3 object '$s3_key' not found.")
        return nothing
    end
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

"""
    _filter_sim_now(
        df::AbstractDataFrame, metadata::S3DBMeta, sim_now::ZonedDateTime
    )::Vector{Int}

Filters the input dataframe for the row with the latest `release_date` up to the
`sim_now` (cutoff) for every group of rows with the same id. The id of a row is the
primary key of the row minus the `release_date` and `tag`.

Returns the indices of the selected rows from the original DataFrame (eg. if a
SubDataFrame was passed in instead of a DataFrame, the indices will map to the row
number in the original DataFrame, not the SubDataFrame).

# Arguments
- `df`: The dataframe to filter
- `metadata`: The S3DB metadata, contains pkey information
- `sim_now`: The sim_now filter
"""
function _filter_sim_now(
    df::AbstractDataFrame, metadata::S3DBMeta, sim_now::ZonedDateTime
)::Vector{Int}
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

    return selections
end

# Factory function to generate a custom-reusable DF filter func
function df_filter_factory(
    filters::Union{Nothing,Dict{Symbol,Vector{P}}},
    excludes::Union{Nothing,Dict{Symbol,Vector{Q}}},
) where {P,Q}
    filters = isnothing(filters) ? Dict() : filters
    excludes = isnothing(excludes) ? Dict() : excludes

    if isempty(filters) && isempty(excludes)
        return nothing
    end

    # Do this conversion once, here, such that they are reusable.
    filters_set = Dict(k => Set(v) for (k, v) in filters)
    excludes_set = Dict(k => Set(v) for (k, v) in excludes)

    function _custom_filter_func(df::DataFrame)
        mask = nothing

        for (items, inclusivity) in ((filters_set, true), (excludes_set, false))
            for (k, v) in items
                # a bit mask for the df column on which rows to keep/trash
                curr_mask = in.(df[!, k], Ref(v)) .== inclusivity
                mask = isnothing(mask) ? curr_mask : mask .& curr_mask
            end
        end

        return @view df[mask, :]
    end

    return _custom_filter_func
end
