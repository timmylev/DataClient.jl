######################################################################
#####   Type Definitions                                          ####
######################################################################

"""
    Store

The (root) abstract type `Store`
"""
abstract type Store end

"""
    Metadata

The (root) abstract type for a dataset's metadata.
"""
abstract type Metadata end

######################################################################
#####   Utility Functions                                         ####
######################################################################

"""
    decode_type(el::AbstractString)::Type
    decode_type(compound::Vector)::Type

Decodes a DataType or Union that was encoded using [`encode_type`](@ref).
"""
function decode_type(el::AbstractString)::Type
    return try
        return haskey(CUSTOM_TYPES, el) ? CUSTOM_TYPES[el] : getfield(Base, Symbol(el))
    catch err
        throw(ErrorException("Custom decoding failed for type '$el'."))
    end
end

function decode_type(compound::Vector)::Type
    return if compound[1] == "Union"
        eval(Expr(:curly, :Union, [decode_type(el) for el in compound[2:end]]...))
    elseif compound[1] == "Array"
        type, dims = compound[2:3]
        Array{decode_type(type),dims}
    elseif compound[1] == "ParametricArray"
        type, dims = compound[2:3]
        Array{T,dims} where {T<:decode_type(type)}
    else
        throw(ErrorException("Custom decoding failed for type '$compound'."))
    end
end

"""
    encode_type(data_type::DataType)::String
    encode_type(data_type::Union)::String

Encodes a DataType or Union as a String.
"""
function encode_type(data_type::DataType)::Union{Vector,String}
    return if nameof(data_type) == :Array
        type, dims = data_type.parameters
        ["Array", encode_type(type), dims]
    elseif data_type in keys(_CUSTOM_TYPES)
        _CUSTOM_TYPES[data_type]
    else
        encoded = repr(data_type)
        if isdefined(Base, Symbol(encoded)) && isempty(data_type.parameters)
            encoded
        else
            throw(ErrorException("Custom encoding failed for type '$data_type'."))
        end
    end
end

function encode_type(data_type::Union)::Vector
    inner_types = [encode_type(el) for el in Base.uniontypes(data_type)]
    return ["Union", inner_types...]
end

function encode_type(data_type::UnionAll)::Vector
    return if data_type.body.name.name == :Array
        dims = data_type.body.parameters[2]
        parametric_type = encode_type(data_type.var.ub)
        ["ParametricArray", parametric_type, dims]
    else
        throw(ErrorException("Custom encoding failed for type '$data_type'."))
    end
end

"""
    sanitize_type(data_type::DataType)::Union{DataType,UnionAll}
    sanitize_type(data_type::UnionAll)::UnionAll
    sanitize_type(data_type::Union)::Union

Converts certain Types (or Union of Types) to their respective AbstractTypes:
- `String`                     -> `AbstractString`
- `Union{Float64,Int}`         -> Union{AbstractFloat,Integer}
- `Vector{Int}`                -> Vector{T} where T <: Integer
- `Vector{Union{Float64,Int}}` -> Vector{T} where T<:Union{AbstractFloat, Integer}


## Developer Note
This is used to generate a column type map for dataset columns. The type map will be
used to validate future insertion of new data (DataFrame).
"""
function sanitize_type(data_type::DataType)::Union{DataType,UnionAll}
    return if data_type.name.name == :Array
        inner_type = sanitize_type(data_type.parameters[1])
        dims = data_type.parameters[2]
        Array{T,dims} where {T<:inner_type}
    elseif data_type <: AbstractString
        AbstractString
    elseif data_type == Bool
        # note that Bool <: Integer
        Bool
    elseif data_type <: Integer
        Integer
    elseif data_type <: AbstractFloat
        AbstractFloat
    else
        data_type
    end
end

function sanitize_type(data_type::UnionAll)::UnionAll
    return if data_type.body.name.name == :Array
        abs_type = sanitize_type(data_type.var.ub)
        dims = data_type.body.parameters[2]
        Array{T,dims} where {T<:abs_type}
    else
        throw(ErrorException("Unable to sanitize type '$data_type'."))
    end
end

function sanitize_type(data_type::Union)::Union
    inner_types = [sanitize_type(el) for el in Base.uniontypes(data_type)]
    return eval(Expr(:curly, [:Union, inner_types...]...))  # reconstructs the Union
end

"""
    unix2zdt(ts::Int, tz::TimeZone=tz"UTC")::ZonedDateTime

Converts a unix timestamp to a `ZonedDateTime`. Function calls are `@memoize`-ed.

## Example
```julia
julia> ts = 1577836800

julia> unix2zdt(ts)
2020-01-01T00:00:00+00:00

julia> unix2zdt(ts, tz"America/New_York")
2019-12-31T19:00:00-05:00
```
"""
@memoize function unix2zdt(ts::Int, tz::TimeZone=tz"UTC")::ZonedDateTime
    dt = unix2datetime(ts)
    return try
        ZonedDateTime(dt, tz; from_utc=true)
    catch err
        (isa(tz, VariableTimeZone) && dt >= tz.cutoff) || throw(err)
        # By default, TimeZones.jl only supports a DateTime that can be represented by an Int32
        # https://juliatime.github.io/TimeZones.jl/stable/faq/#future_tzs-1
        warn(LOGGER, "Unable to localize '$dt' (UTC) as '$tz', falling back to 'UTC'.")
        ZonedDateTime(dt, tz"UTC"; from_utc=true)
    end
end

function s_fmt(s::Union{Integer,AbstractFloat})::Dates.CompoundPeriod
    return canonicalize(Millisecond(trunc(Int, s * 1000)))
end

# used to pretty print nested dicts
function format_dict(d::Dict; offset::Int=1)
    lines = _fmt_dict(d, 0)
    padding = join(fill(" ", offset))
    lines = [padding * l for l in lines]
    return join(lines, "\n")
end

# Depth-first recursion through the Dict
function _fmt_dict(d::Dict, pre::Int)
    lines = []
    max_key_len = max(map(length, map(repr, collect(keys(d))))...)
    padding = join(fill(" ", pre))
    key_format = "$padding{1:$(max_key_len)s} => "
    for (k, v) in d
        line = Format.format(key_format, repr(k))
        if typeof(v) <: Dict  # recurse case
            push!(lines, line)
            append!(lines, _fmt_dict(v, pre + 2))
        else  # base case
            line *= repr(v)
            push!(lines, line)
        end
    end
    return lines
end
