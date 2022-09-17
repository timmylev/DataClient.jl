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
        date::Date,
        store_id::AbstractString;
        strip_tz::Bool=true,
    )::DataFrame

A convenient way to gather datasets using tz-naive `DateTime`s or `Date` instead of
`ZonedDateTime`s. Note that a `store_id` must always be provided when querying this way.

!!! note
    The tz-naive `DateTime`s or `Date` will be converted into `ZonedDateTime`s in
    the background using the dataset's stored timezone.

An additional `strip_tz` parameter is also available, which when `true` (the default),
removes the timezone from all `ZonedDateTime` columns in the retrieved `DataFrame`,
converting the column into a tz-naive `DateTime`.
"""
function gather(
    collection::AbstractString,
    dataset::AbstractString,
    date::Date,
    store_id::AbstractString;
    strip_tz::Bool=true,
)::DataFrame
    start_dt = DateTime(date)
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

    metadata = @mock get_metadata(collection, dataset, store)

    tz = metadata.timezone
    start_zdt = ZonedDateTime(start_dt, tz)
    end_zdt = ZonedDateTime(end_dt, tz)

    df = @mock gather(collection, dataset, start_zdt, end_zdt, store_id)

    if strip_tz
        zdt_cols = _get_zdt_cols(metadata)

        for col in zdt_cols
            # directly strips the timezone without first converting to UTC
            df[!, col] = DateTime.(df[!, col])
        end

        info(
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
