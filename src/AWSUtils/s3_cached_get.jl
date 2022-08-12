"""
    FileCache(maxsize_mb::Int)::FileCache

A file cache used by the [`s3_cached_get`](@ref) function. This is an ephemeral cache
that only persists within the same Julia session.

# Arguments
- `maxsize_mb`: The cache size in MB
"""
struct FileCache
    # This dict stores a mapping of file paths to file sizes in bytes.
    dict::LRU{String,Int64}
    dir::String
end

function FileCache(maxsize_mb::Int)::FileCache
    del_file(k, v) = rm(k)
    maxsize_b = maxsize_mb * 1_000_000
    return FileCache(
        LRU{String,Int64}(; maxsize=maxsize_b, by=identity, finalizer=del_file), mktempdir()
    )
end

# the default cache that is used by the `s3_cached_get` function when a custom cache
# is not provided.
const _DEFAULT_CACHE = Ref{FileCache}()

"""
    s3_cached_get(s3_bucket::String, s3_key::String)::IOStream
    s3_cached_get(s3_bucket::String, s3_key::String, cache::FileCache)::IOStream

A cached version of `AWSS3.s3_get_file()` where downloaded files are cache locally and
re-used for subsequent get requests. Cached files only persist within the same process.

Optionally provide a custom [`FileCache`](@ref). If none is provide, a global cache is
automatically instantiated and used.
"""
function s3_cached_get(s3_bucket::String, s3_key::String)::IOStream
    if !isassigned(_DEFAULT_CACHE)
        _DEFAULT_CACHE[] = FileCache(_DEFAULT_CACHE_SIZE_MB)
    end
    return s3_cached_get(s3_bucket, s3_key, _DEFAULT_CACHE[])
end

function s3_cached_get(s3_bucket::String, s3_key::String, cache::FileCache)::IOStream
    file_path = joinpath(cache.dir, s3_bucket, s3_key)
    get!(cache.dict, file_path) do
        debug(LOGGER, "Downloading S3 file 's3://$s3_bucket/$s3_key'...")
        mkpath(abspath(joinpath(file_path, "..")))
        @mock s3_get_file(s3_bucket, s3_key, file_path)
        filesize(file_path)
    end
    return open(file_path, "r")
end
