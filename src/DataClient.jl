module DataClient

using Arrow
using AWS
using AWSS3
using CSV
using DataFrames
using DataStructures
using Dates
using Exceptions
using Format
using JSON
using Memento
using Memoize
using Mocking
using TimeZones
using TimerOutputs
using TranscodingStreams
using WeakRefStrings
using YAML

include("Configs/Configs.jl")
include("FileFormats/FileFormats.jl")
include("AWSUtils/AWSUtils.jl")

using .Configs
using .FileFormats
using .AWSUtils

export ConfigFileError, DataFrameError, MissingDataError
export reload_backend, get_backend
export list_datasets, gather, insert

export TimeSeriesIndex, HOUR, DAY, MONTH, YEAR
export FileFormats

const LOGGER = getlogger(@__MODULE__)

# register/hardcode any centralized stores here
const CENTRALIZED_STORES = OrderedDict{String,String}(
    "datafeeds" => "s3db:s3://invenia-datafeeds-output/version5/aurora/gz/",
    "datafeeds-arrow" => "s3db-arrow-zst-day:s3://invenia-datafeeds-output/version5/arrow/zst_lv22/day/",
    "public-data" => "ffs:s3://invenia-private-datasets/DataClient/ffs/arrow/zst/",
    "ercot-nda" => "ffs:s3://invenia-ercot-nda-mw6ez7cwz9gtdqw1qejn81bfkdwdouse1a-s3alias/derived_works/DataClient/ffs/arrow/zst/",
    "miso-nda" => "ffs:s3://invenia-miso-nda-5twngkbmrczu6xd9uppda18b5995yuse1a-s3alias/derived_works/DataClient/ffs/arrow/zst/",
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
    return Memento.register(LOGGER; force=true)
end

include("exceptions.jl")
include("common.jl")
include("formats.jl")
include("backends.jl")
include("s3store.jl")
include("list.jl")
include("gather.jl")
include("insert.jl")

include("gather_wrapper.jl")

end # module
