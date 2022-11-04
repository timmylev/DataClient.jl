const BACKENDS = Ref{OrderedDict{String,Store}}(OrderedDict())

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

    return BACKENDS[]
end

function get_backend(store_id::String)::Store
    backend = get_backend()
    return if haskey(backend, store_id)
        backend[store_id]
    else
        throw(ConfigFileError("Store id '$store_id' is not registered."))
    end
end

function _parse_backend_path(backend_uri::String)::Store
    type, uri = split(backend_uri, ":"; limit=2)

    if type in ("s3db", "ffs")
        if startswith(uri, "s3://")
            parts = split(replace(uri, r"^s3://" => ""), "/"; limit=2)
            bucket, prefix = length(parts) == 2 ? parts : (parts[1], "")
            return type == "s3db" ? S3DB(bucket, prefix) : FFS(bucket, prefix)
        else
            throw(
                ConfigFileError(
                    "Invalid uri scheme '$backend_uri' for backend type '$type'"
                ),
            )
        end

    else
        throw(ConfigFileError("Unknown backend type '$type' for '$backend_uri'"))
    end
end
