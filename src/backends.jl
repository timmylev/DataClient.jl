const BACKENDS = Ref{OrderedDict{String,Store}}(OrderedDict())
const BACKENDS_LOCK = ReentrantLock()

"""
    reload_backend()
    reload_backend(path::AbstractString)

Reloads the backend configs from the config file.

Refer to [Configs and Backend](@ref) for more info about config files.
"""
function reload_backend(path::AbstractString)
    Configs.set_config_path(path)
    return reload_backend()
end

function reload_backend()
    empty!(BACKENDS[])
    Configs.reload_configs()
    return get_backend()
end

"""
    get_backend()::OrderedDict{String,Store}
    get_backend(store_id::String)::Store

Gets available backend store(s).

Refer to [Configs and Backend](@ref) for more info about backend stores.
"""
function get_backend()::OrderedDict{String,Store}
    lock(BACKENDS_LOCK) do
        if isempty(BACKENDS[])
            cfg = Configs.get_configs()
            load_order = if haskey(cfg, "additional-stores")
                stores = [collect(d)[1] for d in cfg["additional-stores"]]
                if get(cfg, "disable-centralized", false)
                    [stores]
                elseif get(cfg, "prioritize-additional-stores", false)
                    [stores, CENTRALIZED_STORES]
                else
                    [CENTRALIZED_STORES, stores]
                end
            else
                if get(cfg, "disable-centralized", false)
                    throw(
                        ConfigFileError(
                            "Do not set `disable-centralized: True` in the config file " *
                            "when no `additional-stores` are defined.",
                        ),
                    )
                else
                    [CENTRALIZED_STORES]
                end
            end

            for stores in load_order
                for (name, uri) in stores
                    if !haskey(BACKENDS[], name)
                        global BACKENDS[][name] = _parse_backend_path(uri)
                    end
                end
            end

            ids = keys(BACKENDS[])
            debug(LOGGER, "Loaded $(length(ids)) backend store(s): $(ids).")
        end
    end

    return BACKENDS[]
end

function get_backend(store_id::String)::Store
    backend = get_backend()
    return if haskey(backend, store_id)
        backend[store_id]
    else
        # If it is not registered, attempt to parse it incase it is a URI
        try
            _parse_backend_path(store_id)
        catch err
            rethrow(
                ConfigFileError("Store id/uri '$store_id' is not registered or is invalid.")
            )
        end
    end
end

function _parse_backend_path(backend_uri::AbstractString)::Store
    type, uri = split(backend_uri, ":"; limit=2)

    return if type == "ffs"
        bucket, prefix = _parse_s3_uri(uri)
        FFS(bucket, prefix)

    elseif type == "s3db"
        bucket, prefix = _parse_s3_uri(uri)
        S3DB(bucket, prefix)

    elseif startswith(type, "s3db")
        bucket, prefix = _parse_s3_uri(uri)
        _, file_format, compression, partition = split(type, "-")
        S3DB(bucket, prefix, partition, file_format, compression)

    else
        throw(ConfigFileError("Unknown backend type '$type' for '$backend_uri'"))
    end
end

function _parse_s3_uri(uri::AbstractString)
    return if startswith(uri, "s3://")
        parts = split(replace(uri, r"^s3://" => ""), "/"; limit=2)
        bucket, prefix = length(parts) == 2 ? parts : (parts[1], "")
    else
        throw(ConfigFileError("Invalid uri scheme '$uri'"))
    end
end
