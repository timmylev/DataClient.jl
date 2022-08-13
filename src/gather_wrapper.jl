"""
    gather(
        collection::AbstractString,
        dataset::AbstractString,
        start_dt::DateTime,
        end_dt::DateTime,
        store_id::AbstractString;
        strip_tz::Bool=true,
    )::DataFrame
    gather(
        collection::AbstractString,
        dataset::AbstractString,
        day::Date,
        store_id::AbstractString;
        strip_tz::Bool=true,
    )::DataFrame

An alternative way to gather datasets using tz-naive `DateTime`s or `Date`.

A `store_id` must always be specified. If `strip_tz=true`, any `ZonedDateTime`
columns in the retrieved `DataFrame` will be converted into tz-naive `DateTime`s.
"""
function gather(
    collection::AbstractString,
    dataset::AbstractString,
    day::Date,
    store_id::AbstractString;
    strip_tz::Bool=true,
)::DataFrame
    start_dt = DateTime(day)
    end_dt = start_dt + Day(1) - Second(1)
    return gather(collection, dataset, start_dt, end_dt, store_id; strip_tz=strip_tz)
end

function gather(
    collection::AbstractString,
    dataset::AbstractString,
    start_dt::DateTime,
    end_dt::DateTime,
    store_id::AbstractString;
    strip_tz::Bool=true,
)::DataFrame
    store = get_backend(store_id)

    metadata = try
        get_metadata(collection, dataset, store)
    catch err
        throw(MissingDataError(collection, dataset))
    end

    tz = metadata.timezone
    start_zdt = ZonedDateTime(start_dt, tz)
    end_zdt = ZonedDateTime(end_dt, tz)

    df = gather(collection, dataset, start_zdt, end_zdt, store_id)

    if strip_tz
        zdt_cols = _get_zdt_cols(metadata)

        for col in zdt_cols
            # directly strips the timezone without first converting to UTC
            df[!, col] = DateTime.(df[!, col])
        end

        warn(
            LOGGER,
            "Stripped the tz '$tz' from `ZonedDateTime` columns $zdt_cols to create " *
            "tz-naive (non-UTC) `DateTime` columns for dataset '$(collection)-$(dataset)'.",
        )
    end

    return df
end

function _get_zdt_cols(metadata::FFSMeta)::Vector{String}
    return [k for (k, v) in pairs(metadata.column_types) if v == ZonedDateTime]
end

function _get_zdt_cols(metadata::S3DBMeta)::Vector{String}
    return ["target_start", "target_end", "release_date"]
end
