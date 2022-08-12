using .AWSUtils: s3_list_dirs

"""
    list_datasets()::Dict{String,Dict{String,Vector{String}}}

Lists all available datasets from all collections in all registered stores.

Note that datasets are grouped into collections, and each store can have multiple
collections. Centralized stores such as S3DB from Datafeeds are already automatically
registered. Additional stores can be registered in the `configs.yaml` file.
"""
function list_datasets()::Dict{String,Dict{String,Vector{String}}}
    results = Dict{String,Dict{String,Vector{String}}}()

    for (store_id, store) in pairs(get_backend())
        results[store_id] = _list(store)
    end

    return results
end

"""
    list_datasets(store_id::AbstractString)::Dict{String,Vector{String}}

Lists all available datasets from all collections in the target store.
"""
function list_datasets(store_id::AbstractString)::Dict{String,Vector{String}}
    return _list(get_backend(store_id))
end

"""
    list_datasets(store_id::AbstractString, collection::AbstractString)::Vector{String}

Lists all available datasets from the target collection and stores.
"""
function list_datasets(store_id::AbstractString, collection::AbstractString)::Vector{String}
    return _list(get_backend(store_id), collection)
end

function _list(store::S3Store)::Dict{String,Vector{String}}
    results = Dict{String,Vector{String}}()

    asyncmap(@mock s3_list_dirs(store.bucket, store.prefix); ntasks=8) do collection
        results[collection] = _list(store, collection)
    end

    return results
end

function _list(store::S3Store, collection::AbstractString)::Vector{String}
    collection_path = joinpath(store.prefix, collection, "")  # add trailing '/'
    return [ds for ds in @mock s3_list_dirs(store.bucket, collection_path)]
end
