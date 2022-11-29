#####################################################################################
########          File Formats                                         ##############
#####################################################################################
"""
    load_df(
        data::Union{AbstractString,Vector{UInt8}},
        fmt::FileFormat,
        compression::Union{Compression,Nothing}=nothing,
    )::DataFrame

Loads the DataFrame from a file based on the [`FileFormats.FileFormat`](@ref) and
[`FileFormats.Compression`](@ref).
"""
function load_df(
    data::Union{AbstractString,Vector{UInt8}},
    fmt::FileFormat,
    compression::Union{Compression,Nothing}=nothing,
)::DataFrame
    if !isnothing(compression)
        data = _decompress(data, compression)
    end

    return _load_df(data, Val(fmt))
end

function _load_df(
    data::Union{AbstractString,Vector{UInt8}}, ::Val{FileFormats.CSV}
)::DataFrame
    # CSV@0.10.7 broke multithreaded parsing within asyncmap, restrict to ntasks=1 for now.
    return CSV.read(data, DataFrame; ntasks=1, stringtype=String)
end

function _load_df(
    data::Union{AbstractString,Vector{UInt8}}, ::Val{FileFormats.ARROW}
)::DataFrame
    # Note: these are memory mapped Arrow primitives, conversion to julia types will be
    # done later down the pipeline.
    return DataFrame(Arrow.Table(data))
end

function _decompress(data::Vector{UInt8}, compression::Compression)::Vector{UInt8}
    return transcode(DECOMPRESSION_CODEC[compression], data)
end

function _decompress(file::AbstractString, compression::Compression)::Vector{UInt8}
    open(file, "r") do fp
        _decompress(read(fp), compression)
    end
end

"""
    write_df(
        df::AbstractDataFrame, fmt::FileFormat, compression::Union{Compression,Nothing}=nothing
    )::Vector{UInt8}

Writes a DataFrame to a `Vector{UInt8}` based on the specified [`FileFormats.FileFormat`](@ref)
and [`FileFormats.Compression`](@ref).
"""
function write_df(
    df::AbstractDataFrame, fmt::FileFormat, compression::Union{Compression,Nothing}=nothing
)::Vector{UInt8}
    data = _write_df(df, Val(fmt))
    return isnothing(compression) ? data : transcode(COMPRESSION_CODEC[compression], data)
end

function _write_df(df::AbstractDataFrame, ::Val{FileFormats.CSV})::Vector{UInt8}
    stream = IOBuffer()
    # Vectors are non-standard types in CSV, attempt to json-encode them.
    # We'll make a copy to not mutate the original DataFrame.
    vector_cols = [col for col in names(df) if eltype(df[!, col]) <: Vector]

    if !isempty(vector_cols)
        other_cols = setdiff(names(df), vector_cols)
        df_copy = select(df, other_cols; copycols=false)
        for col in vector_cols
            df_copy[!, col] = JSON.json.(df[!, col])
        end
        df = select!(df_copy, names(df)) # reorder columns
    end

    CSV.write(stream, df)
    return take!(stream)
end

function _write_df(df::AbstractDataFrame, ::Val{FileFormats.ARROW})::Vector{UInt8}
    stream = IOBuffer()
    Arrow.write(stream, df)
    return take!(stream)
end
