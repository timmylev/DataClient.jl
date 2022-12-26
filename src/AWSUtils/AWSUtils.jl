module AWSUtils

using AWS
using AWS.AWSExceptions: AWSException
using AWSS3
using Dates
using Memento
using Mocking
using LRUCache
using TranscodingStreams

using ..Configs
using ..FileFormats

# The default LRU cache size for the s3_cached_get() function
const _DEFAULT_CACHE_SIZE_MB = 20000
const _DEFAULT_CACHE_EXPIRY_DAYS = 90

const LOGGER = getlogger(@__MODULE__)

function __init__()
    return Memento.register(LOGGER)
end

@service S3

include("s3_cached_get.jl")
include("s3_list_dirs.jl")

end # module
