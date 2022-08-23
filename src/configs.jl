const CONFIG_PATH = Ref{String}()
const BACKENDS = Ref{OrderedDict{String,Store}}(OrderedDict())

"""
    reload_configs()

Reloads the config file using the latest config file path into memory.

Refer to [Configs and Backend](@ref) for more info about config files.
"""
function reload_configs()
    empty!(BACKENDS[])
    get_backend()
    return nothing
end

"""
    reload_configs(path::String)

Updates the config file path and reloads the config file into memory.

Refer to [Configs and Backend](@ref) for more info about config files
"""
function reload_configs(path::String)
    _set_config_path!(path)
    reload_configs()
    return nothing
end

"""
    _set_config_path!(path::String)

Updates the config file path.
"""
function _set_config_path!(path::String)
    global CONFIG_PATH[] = path
    return empty!(BACKENDS[])
end

"""
    _get_config_path()

Gets the config file path.
"""
_get_config_path() = CONFIG_PATH[]

"""
    get_backend()::OrderedDict{String,Store}

Gets all registered backend stores and their respective configs.

Refer to [Configs and Backend](@ref) for more info about backend stores.
"""
function get_backend()::OrderedDict{String,Store}
    if isempty(BACKENDS[])
        cfg_path = _get_config_path()
        cfg = if isfile(cfg_path)
            debug(LOGGER, "Loading config file '$cfg_path'...")
            try
                YAML.load_file(_get_config_path())
            catch err
                throw(ConfigFileError("Loading config file '$(_get_config_path())' failed."))
            end
        else
            info(LOGGER, "Config file '$cfg_path' is not available, using default stores.")
            Dict()
        end

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

"""
    get_backend(store_id::String)::Store

Gets the backend store configs of the given store id.

Refer to [Configs and Backend](@ref) for more info about backend stores.
"""
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
