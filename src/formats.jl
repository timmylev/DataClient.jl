
#####################################################################################
########          File Formats                                         ##############
#####################################################################################

"""
    StorageFormat

Storage format options Enum for [`FFS`](@ref)

# Enum Values
- CSV_GZ
"""
@enum StorageFormat CSV_GZ
const _STORAGE_FORMAT = Dict(string.(instances(StorageFormat)) .=> instances(StorageFormat))
const _EXTENSION = Dict(CSV_GZ => "csv.gz")
StorageFormat(str::AbstractString)::StorageFormat = _STORAGE_FORMAT[str]
value(key::StorageFormat) = _EXTENSION[key]

"""
    load_df(fmt::StorageFormat, file)::DataFrame

Loads a file into a DataFrame given a [`StorageFormat`](@ref). The type of `file` will
be implementation specific but should typically work on paths and streams.
"""
load_df(fmt::StorageFormat, file)::DataFrame = _load_df(Val(fmt), file)

"""
    write_df(fmt::StorageFormat, df::AbstractDataFrame)::Vector{UInt8}

Write a DataFrame into a `Vector{UInt8}` given a [`StorageFormat`](@ref).
"""
write_df(fmt::StorageFormat, df::AbstractDataFrame)::Vector{UInt8} = _write_df(Val(fmt), df)

_load_df(::Val{CSV_GZ}, file)::DataFrame = CSV.read(file, DataFrame)

function _write_df(::Val{CSV_GZ}, df::AbstractDataFrame)::Vector{UInt8}
    stream = IOBuffer()
    CSV.write(stream, df; compress=true)
    return stream.data
end
