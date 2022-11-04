module Configs

using Memento
using YAML

LOGGER = getlogger(@__MODULE__)

const CONFIG_PATH = Ref{String}()
# choosing `nothing` over `#undef` becuase this makes it possible to "unassign" the ref
const CONFIGS = Ref{Union{Dict{String,Any},Nothing}}(nothing)

const CUSTOM_ENVS = [
    ("DATACLIENT_CACHE_DECOMPRESS", Bool)
    ("DATACLIENT_CACHE_DIR", String)
    ("DATACLIENT_CACHE_EXPIRE_AFTER_DAYS", Int)
    ("DATACLIENT_CACHE_SIZE_MB", Int)
]

function __init__()
    set_config_path(joinpath(pwd(), "configs.yaml"))
    return Memento.register(LOGGER)
end

function get_configs()
    return isnothing(CONFIGS[]) ? reload_configs() : CONFIGS[]
end

function set_config_path(path::String)
    return CONFIG_PATH[] = path
end

function reload_configs(cfg_path::AbstractString)
    set_config_path(cfg_path)
    return reload_configs()
end

function reload_configs()
    cfg_path = CONFIG_PATH[]

    if isfile(cfg_path)
        CONFIGS[] = YAML.load_file(cfg_path)
        debug(LOGGER, "Loaded configs $(CONFIGS[]) from file '$cfg_path'.")
    else
        trace(LOGGER, "Config file '$cfg_path' is not available, resetting configs.")
        CONFIGS[] = Dict()
    end

    # ENVs will override config file
    for (key, type) in CUSTOM_ENVS
        if haskey(ENV, key)
            CONFIGS[][key] = ENV[key]
            trace(LOGGER, "Loaded ENV '$key' with val $(CONFIGS[][key])")
        end

        if haskey(CONFIGS[], key) && type != typeof(CONFIGS[][key])
            CONFIGS[][key] = parse(type, CONFIGS[][key])
        end
    end

    return CONFIGS[]
end

end # module
