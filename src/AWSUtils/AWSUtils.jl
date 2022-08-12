module AWSUtils

using AWS
using AWSS3
using Memento
using Mocking
using LRUCache

# The default LRU cache size for the s3_cached_get() function
const _DEFAULT_CACHE_SIZE_MB = 2000

const LOGGER = getlogger(@__MODULE__)

function __init__()
    return Memento.register(LOGGER)
end

@service S3

include("s3_cached_get.jl")
include("s3_list_dirs.jl")

end # module
