"""
    FileCache(
        maxsize_mb::Int;
        cache_dir::Union{AbstractString,Nothing}=nothing,
        cache_dir_ttl::Union{Day,Nothing}=nothing,
    )::FileCache

An LRU file cache used by the [`s3_cached_get`](@ref) function.

# Arguments
- `maxsize_mb`: The max cache size in MB before files are de-registered from the LRU

# Keywords
- `cache_dir`: If provided, the cache is reconstructed using any pre-existing files in
    this directory. Otherwise, a new temporary directory is automatically created for
    the current session.
- `cache_dir_ttl`: The "time-to-live" for files in the provided `cache_dir` (so this is
    only relevant when the `cache_dir` param is specified). When the cache is instantiated
    with a pre-existing `cache_dir`, files in the `cache_dir` that is older than this
    period will be deleted.

!!! note "WARNING: Multi-process Shared Cache"
    The [`FileCache`](@ref) (LRU-based) is thread-safe, but not multi-process safe.
    When sharing the same (custom) cache directory across multiple processes, there may
    be cases where one proccess starts removing file(s) from the cache dir due to it
    hitting the max cache size limit. This is a problem for the other processes because
    the removed file(s) are still registered in their cache registries even though the
    underlying files no longer exists. To reduce the likelyhood of this happening, set
    the max cache size limit to a high number such that file removals do not happen.
    Alternatively, do not use a custom cache dir for multi-process use cases and stick
    with the default ephemeral cache instead, which creates a separate cache dir for
    each process.

"""
struct FileCache
    # This dict stores a mapping of file paths to file sizes in bytes.
    dict::LRU{String,Int64}
    dir::String
end

function FileCache(
    maxsize_mb::Int;
    cache_dir::Union{AbstractString,Nothing}=nothing,
    cache_dir_ttl::Union{Day,Nothing}=nothing,
)::FileCache
    del_file(k, v) = rm(k)  # deletes files from disk on expiry
    maxsize_b = maxsize_mb * 1_000_000
    lru = LRU{String,Int64}(; maxsize=maxsize_b, by=identity, finalizer=del_file)

    if !isnothing(cache_dir)
        cache_dir = mkpath(abspath(normpath(cache_dir)))  # mkpath if not exist
        all_files = [  # reconstruct LRU if needed
            joinpath(base, name) for (base, dirs, files) in walkdir(cache_dir) for
            name in files
        ]
        debug(LOGGER, "Detected $(length(all_files)) file(s) in cache dir '$(cache_dir)'")

        # remove expired files
        if !isnothing(cache_dir_ttl)
            now_unix = datetime2unix(@mock now())
            ttl = Dates.value(Second(cache_dir_ttl))
            to_remove = filter(f -> now_unix - mtime(f) > ttl, all_files)

            if !isempty(to_remove)
                debug(LOGGER, "Removing $(length(to_remove)) stale cached files.")
                setdiff!(all_files, to_remove)
                map(rm, to_remove)  # clean up cache dir
            end
        end

        # sort by date before reconstructing the LRU
        for file_path in sort!(all_files; by=f -> mtime(f))
            lru[file_path] = filesize(file_path)
        end
    else
        cache_dir = mktempdir()
    end

    return FileCache(lru, cache_dir)
end

# the default cache that is used by the `s3_cached_get` function when a custom cache
# is not provided.
const _DEFAULT_CACHE = Ref{Union{FileCache,Nothing}}(nothing)

unset_global_cache() = _DEFAULT_CACHE[] = nothing

function get_global_cache()::FileCache
    if isnothing(_DEFAULT_CACHE[])
        configs = Configs.get_configs()
        cache_dir = get(configs, "DATACLIENT_CACHE_DIR", nothing)
        cache_size = get(configs, "DATACLIENT_CACHE_SIZE_MB", _DEFAULT_CACHE_SIZE_MB)
        cache_ttl = Day(
            get(configs, "DATACLIENT_CACHE_EXPIRE_AFTER_DAYS", _DEFAULT_CACHE_EXPIRY_DAYS)
        )

        if !isnothing(cache_dir)
            cache_dir = abspath(normpath(cache_dir))
            info(LOGGER, "Using persistent cache dir '$cache_dir'")
        end

        _DEFAULT_CACHE[] = FileCache(
            cache_size; cache_dir=cache_dir, cache_dir_ttl=cache_ttl
        )
    end

    return _DEFAULT_CACHE[]
end

"""
    s3_cached_get(s3_bucket::String, s3_key::String; decompress::Bool=true)::String
    s3_cached_get(s3_bucket::String, s3_key::String, cache::FileCache; decompress::Bool=true)::String

A cached version of `AWSS3.s3_get_file()` where downloaded files are cache locally and
re-used for subsequent get requests. The local file path for the cached file is returned.

By default, downloaded files are automatically decompressed before being cached if the
file extension indicates that the file has been compressed. This is only supported for
certain types of compression. Set `decompress=false` to disable this feature.

If `cache` is not provide, a global cache is automatically instantiated. A Config file
and/or environment variables can be used to supply the arguments used to instantiate the
[`FileCache`](@ref), defaults are used if none is provided.
- `DATACLIENT_CACHE_DIR` (`String`): The absolute path to a custom directory to be used
    as the cache. Files cached here will be persistent, i.e. not removed at the end of
    the session but may be removed during runtime.
- `DATACLIENT_CACHE_SIZE_MB` (`Int`): The max cache size in MB before files are
    de-registered/removed from the LRU during the session in real-time.
- `DATACLIENT_CACHE_EXPIRE_AFTER_DAYS` (`Int`): This is only relevant when initializing
    the cache at the start of the session if a custom cache dir is specified. Files in
    the cache dir that is older than this period will be removed during initialisation.
    The default is 90 days.

!!! note "WARNING: Multi-process Shared Cache"
    The [`FileCache`](@ref) (LRU-based) is thread-safe, but not multi-process safe.
    When sharing the same (custom) cache directory across multiple processes, there may
    be cases where one proccess starts removing file(s) from the cache dir due to it
    hitting the max cache size limit. This is a problem for the other processes because
    the removed file(s) are still registered in their cache registries even though the
    underlying files no longer exists. To reduce the likelyhood of this happening, set
    the max cache size limit to a high number such that file removals do not happen.
    Alternatively, do not use a custom cache dir for multi-process use cases and stick
    with the default ephemeral cache instead, which creates a separate cache dir for
    each process.
"""
function s3_cached_get(s3_bucket::String, s3_key::String; decompress::Bool=true)::String
    return s3_cached_get(s3_bucket, s3_key, get_global_cache(); decompress=decompress)
end

function s3_cached_get(
    s3_bucket::String, s3_key::String, cache::FileCache; decompress::Bool=true
)::String
    file_format, compression = FileFormats.detect_format(s3_key)
    codec = decompress ? get(DECOMPRESSION_CODEC, compression, nothing) : nothing

    # if decompressing, remove the compression extension from cached files
    cached_suffix = isnothing(codec) ? s3_key : splitext(s3_key)[1]
    cached_path = joinpath(cache.dir, s3_bucket, cached_suffix)

    get!(cache.dict, cached_path) do
        trace(LOGGER, "Downloading S3 file 's3://$s3_bucket/$s3_key'...")

        data = nothing
        MAX_ATTEMPTS = 3
        attempts = 0
        while true
            attempts += 1
            try
                data = @mock s3_get(s3_bucket, s3_key; retry=false)
                break
            catch err
                # Does retries for non-AWSExceptions
                # https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/issues/20
                if isa(err, AWSException) || attempts >= MAX_ATTEMPTS
                    throw(err)
                end
            end
        end

        if !isnothing(codec)
            data = @mock transcode(codec, data)
        end

        mkpath(dirname(cached_path))
        open(cached_path, "w") do fp
            write(fp, data)
        end

        filesize(cached_path)  # LRU dict value, keeps track of cache size.
    end

    return cached_path
end
