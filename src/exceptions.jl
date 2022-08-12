abstract type AbstractError <: Exception end

Base.showerror(io::IO, e::AbstractError) = print(io, "$(typeof(e)): $(e.msg)")

mutable struct ConfigFileError <: AbstractError
    msg::String
end

mutable struct DataFrameError <: AbstractError
    msg::String
end

mutable struct MissingDataError <: AbstractError
    msg::String
end

function MissingDataError(
    coll::AbstractString, ds::AbstractString, start_dt::ZonedDateTime, end_dt::ZonedDateTime
)::MissingDataError
    return MissingDataError(
        "Dataset '$(coll)-$(ds)' on range [$start_dt, $end_dt] not found."
    )
end

function MissingDataError(coll::AbstractString, ds::AbstractString)::MissingDataError
    return MissingDataError("Dataset '$coll-$ds' not found.")
end
