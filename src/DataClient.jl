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
using YAML

export ConfigFileError, DataFrameError, MissingDataError
export reload_configs, get_backend
export list_datasets, gather, insert

const LOGGER = getlogger(@__MODULE__)

# register/hardcode any centralized stores here
const CENTRALIZED_STORES = OrderedDict{String,String}(
    "datafeeds" => "s3db:s3://invenia-datafeeds-output/version5/aurora/gz/",
    "public-data" => "ffs:s3://invenia-private-datasets/DataClient/V1/",
    "miso-nda" => "ffs:s3://invenia-miso-nda-5twngkbmrczu6xd9uppda18b5995yuse1a-s3alias/derived_works/DataClient/V1/",
)
# https://gitlab.invenia.ca/invenia/TabularDataSchema/-/blob/master/versions/2017-05-02_001.md
const BOUNDS = Dict(0 => "()", 1 => "[)", 2 => "(]", 3 => "[]")

const TIMEZONES = Dict(
    "caiso" => tz"America/Los_Angeles",
    "ercot" => tz"America/Chicago",
    "pjm" => tz"America/New_York",
    "iso_ne" => tz"America/New_York",
    "miso" => tz"EST",  # fixed tz
    "nyiso" => tz"America/New_York",
    "spp" => tz"America/Chicago",
)

const CUSTOM_TYPES = Dict("ZonedDateTime" => ZonedDateTime, "DateTime" => DateTime)

function get_tz(coll::AbstractString, ds::AbstractString)::TimeZone
    # Special case: Datasoup data that is processed by datafeeds will be grouped
    # under the 'datasoup' collection with '<grid>-' as the dataset prefix in S3DB.
    # eg: 'version5/aurora/gz/datasoup/caiso-dataset_1_from_datasoup/'
    coll = coll == "datasoup" ? split(ds, "-"; limit=2)[1] : coll
    return get(TIMEZONES, "$coll-$ds", get(TIMEZONES, "$coll", tz"UTC"))
end

function __init__()
    _set_config_path!(joinpath(pwd(), "configs.yaml"))
    return Memento.register(LOGGER; force=true)
end

include("AWSUtils/AWSUtils.jl")

include("exceptions.jl")
include("common.jl")
include("configs.jl")
include("list.jl")
include("gather.jl")
include("insert.jl")

include("gather_wrapper.jl")

end # module
