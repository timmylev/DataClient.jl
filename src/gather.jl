using .AWSUtils: s3_cached_get
using AWS.AWSExceptions: AWSException
using TimeZones: zdt2unix

"""
    gather(
        collection::AbstractString,
        dataset::AbstractString,
        start_dt::ZonedDateTime,
        end_dt::ZonedDateTime,
        [store_id::AbstractString,]
    )::DataFrame

Gathers data from a target dataset as a `DataFrame`.

It is recommended to specify a `store_id` for efficiency reasons. If none is provided,
each available store will be iteratively checked in order of precedence until the first
store containing the target dataset is found. Refer to [Configs and Backend](@ref) for
more info about store precedence.

!!! note "IMPORTANT"
    The `start_dt` and `end_dt` filters are only applied to the `target_start` column of
    the dataset. The `target_end` column is irrelevant.
"""
function gather(
    collection::AbstractString,
    dataset::AbstractString,
    start_dt::ZonedDateTime,
    end_dt::ZonedDateTime,
)::DataFrame
    # get_backend() returns an OrderedDict, i.e. the search order in configs.yaml
    for (name, store) in pairs(get_backend())
        data = _gather(collection, dataset, start_dt, end_dt, store)

        if !isempty(data)
            return data
        end
    end

    throw(MissingDataError(collection, dataset, start_dt, end_dt))
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
    collection::AbstractString,
    dataset::AbstractString,
    start_dt::ZonedDateTime,
    end_dt::ZonedDateTime,
    store::S3Store,
)::DataFrame
    # - If grabbing many files, do a s3-list first to filter out missing files.
    # - If grabbing a few files, it's faster to skip the list and attempt to s3-get directly.
    file_keys = if end_dt - start_dt > Day(10)
        @mock _find_s3_files(collection, dataset, start_dt, end_dt, store)
    else
        @mock _generate_keys(collection, dataset, start_dt, end_dt, store)
    end

    return @mock _load_s3_files(file_keys, collection, dataset, start_dt, end_dt, store)
end

function _find_s3_files(
    collection::AbstractString,
    dataset::AbstractString,
    start_dt::ZonedDateTime,
    end_dt::ZonedDateTime,
    store::S3Store,
)::Vector{String}
    collection_prefix = joinpath(store.prefix, collection, dataset, "")

    debug(
        LOGGER,
        "Searching for dataset '$(collection)-$(dataset)' on range " *
        "[$(start_dt), $(end_dt)] in '$(collection_prefix)'...",
    )

    # - S3DB data is partitioned into 24-hr files
    # - S3DB files are named after unix timestamps (start of day utc)
    # - S3DB files are grouped into yearly directories
    start_day_utc = utc_day_floor(start_dt)
    end_day_utc = utc_day_floor(end_dt)
    start_day_unix = zdt2unix(Int, start_day_utc)
    end_day_unix = zdt2unix(Int, end_day_utc)

    dataset_dirs = [
        joinpath(collection_prefix, "year=$(year)", "") for
        year in range(Dates.year(start_day_utc), Dates.year(end_day_utc); step=1)
    ]

    file_keys = Vector{String}()

    # list all files in the yearly directories and filter for matches.
    for dir in dataset_dirs
        for key in @mock s3_list_keys(store.bucket, dir)
            file_date = get_s3_file_timestamp(key)
            if file_date >= start_day_unix && file_date <= end_day_unix
                push!(file_keys, key)
            end
        end
    end

    debug(
        LOGGER,
        "Found $(length(file_keys)) files for dataset '$(collection)-$(dataset)' in " *
        "range [$(start_dt), $(end_dt)]\n - $(join(file_keys, "\n - "))",
    )

    return file_keys
end

function _generate_keys(
    collection::AbstractString,
    dataset::AbstractString,
    start_dt::ZonedDateTime,
    end_dt::ZonedDateTime,
    store::S3Store,
)::Vector{String}
    collection_prefix = joinpath(store.prefix, collection, dataset, "")

    # - S3DB data is partitioned into 24-hr files
    # - S3DB files are named after unix timestamps (start of day utc)
    # - S3DB files are grouped into yearly directories
    file_keys = [
        generate_s3_file_key(collection, dataset, dt, store) for
        dt in range(utc_day_floor(start_dt), utc_day_floor(end_dt); step=Day(1))
    ]

    debug(
        LOGGER,
        "Generated $(length(file_keys)) keys for dataset '$(collection)-$(dataset)' in" *
        " range [$(start_dt), $(end_dt)]\n - $(join(file_keys, "\n - "))",
    )

    return file_keys
end

function _load_s3_files(
    file_keys::Vector{String},
    collection::AbstractString,
    dataset::AbstractString,
    start_dt::ZonedDateTime,
    end_dt::ZonedDateTime,
    store::S3Store,
)::DataFrame
    dfs = Dict{Int,DataFrame}()

    start_unix = zdt2unix(Int, start_dt)
    end_unix = zdt2unix(Int, end_dt)
    start_day_unix = zdt2unix(Int, utc_day_floor(start_dt))
    end_day_unix = zdt2unix(Int, utc_day_floor(end_dt))

    metadata = get_metadata(collection, dataset, store)

    asyncmap(file_keys; ntasks=8) do key
        file = nothing

        try
            file = @mock s3_cached_get(store.bucket, key)
        catch err
            isa(err, AWSException) && err.code == "NoSuchKey" || throw(err)
            debug(LOGGER, "S3 object '$key' not found.")
        finally
            if !isnothing(file)
                df = DataFrame(CSV.File(file))

                # trim the first as last files if they contain extra data
                file_date = get_s3_file_timestamp(key)
                if file_date == start_day_unix || file_date == end_day_unix
                    filter!(:target_start => ts -> ts >= start_unix && ts <= end_unix, df)
                end

                _process_dataframe!(df, metadata)

                dfs[file_date] = df
            end
        end
    end

    return if !isempty(dfs)
        reduce(vcat, (dfs[key] for key in sort(collect(keys(dfs)))))
    else
        DataFrame()
    end
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

    # convert Int bounds to intervals notation, eg. "[)"
    df.target_bounds = [BOUNDS[b] for b in df.target_bounds]

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
