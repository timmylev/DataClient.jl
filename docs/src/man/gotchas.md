# Gotchas

## 1. File Caching
DataClient.jl uses S3-Buckets as its backend storage, and S3-Object caching within the same Julia session is implemented for the [`gather`](@ref) operation.
This means that any S3-Objects (data and metadata) downloaded by the `gather` operation are cached locally for future use within the same Julia session.  
This also means that any modifications (via the [`insert`](@ref) operation) to an existing dataset may not be picked up by the `gather` operation if the relevant (stale) S3-Object(s) were previously cached.
This is only applicable to operations within the same Julia session because cached files do not persist across Julia sessions.
A better cache invalidation strategy may be implemented in the future but this will be the case for the time being.
