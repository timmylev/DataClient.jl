module DataClient

using AWS
using AWSS3
using CSV
using DataFrames
using DataStructures
using Dates
using Exceptions
using JSON
using Memento
using Memoize
using Mocking
using TimeZones
using TimerOutputs
using WeakRefStrings
using YAML

export ConfigFileError, DataFrameError, MissingDataError
export reload_configs, get_backend
export list_datasets, gather, insert

export TimeSeriesIndex, HOUR, DAY, MONTH, YEAR
export CSV_GZ

const LOGGER = getlogger(@__MODULE__)

# register/hardcode any centralized stores here
const CENTRALIZED_STORES = OrderedDict{String,String}(
    "datafeeds" => "s3db:s3://invenia-datafeeds-output/version5/aurora/gz/",
    "public-data" => "ffs:s3://invenia-private-datasets/DataClient/FFS/CSV_GZ/",
    "miso-nda" => "ffs:s3://invenia-miso-nda-5twngkbmrczu6xd9uppda18b5995yuse1a-s3alias/derived_works/DataClient/FFS/CSV_GZ/",
)
# https://gitlab.invenia.ca/invenia/TabularDataSchema/-/blob/master/versions/2017-05-02_001.md
const BOUNDS = Dict{Int,String3}(0 => "()", 1 => "[)", 2 => "(]", 3 => "[]")

const TIMEZONES = Dict(
    "caiso" => tz"America/Los_Angeles",
    "ercot" => tz"America/Chicago",
    "pjm" => tz"America/New_York",
    "iso_ne" => tz"America/New_York",
    "miso" => tz"EST",  # fixed tz
    "nyiso" => tz"America/New_York",
    "spp" => tz"America/Chicago",
)

const CUSTOM_TYPES = Dict(
    "ZonedDateTime" => ZonedDateTime, "DateTime" => DateTime, "Date" => Date
)

const _CUSTOM_TYPES = Dict(values(CUSTOM_TYPES) .=> keys(CUSTOM_TYPES))

function get_tz(coll::AbstractString, ds::AbstractString)::TimeZone
    # Special case: Datasoup data that is processed by datafeeds will be grouped
    # under the 'datasoup' collection with '<grid>_' as the dataset prefix in S3DB.
    # eg: 'version5/aurora/gz/datasoup/caiso_dataset_1_from_datasoup/'
    match = filter(grid -> startswith(ds, grid), collect(keys(TIMEZONES)))
    coll = coll == "datasoup" && !isempty(match) ? only(match) : coll
    return get(TIMEZONES, "$coll-$ds", get(TIMEZONES, "$coll", tz"UTC"))
end

function __init__()
    _set_config_path!(joinpath(pwd(), "configs.yaml"))
    return Memento.register(LOGGER; force=true)
end

include("AWSUtils/AWSUtils.jl")

include("exceptions.jl")
include("common.jl")
include("formats.jl")
include("configs.jl")
include("s3store.jl")
include("list.jl")
include("gather.jl")
include("insert.jl")

include("gather_wrapper.jl")

end # module
