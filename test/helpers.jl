using AWS.AWSExceptions: AWSException
using Dates
using HTTP: StatusError

AwsKeyErr = AWSException("NoSuchKey", "", "", StatusError(404, "GET", "", ""))
AwsOtherErr = AWSException("OtherError", "", "", StatusError(404, "GET", "", ""))

dt2unix(dt::DateTime) = convert(Int, datetime2unix(dt))
dt2unix(args...) = convert(Int, datetime2unix(DateTime(args...)))

"""
    get_test_data(s3_key::String, ex=nothing)::String

Loads in a test file from 'test/files/s3db_data/' when given an S3 key.
This is used to mock the S3 GET operations.
Optionally provide an exception to throw if a file does not exist.
"""
function get_test_data(s3_key::String, ex=nothing)::String
    path = joinpath(@__DIR__, "files", "data", s3_key)

    if !isfile(path)
        ex = isnothing(ex) ? ErrorException("Test file $path does not exist.") : ex
        throw(ex)
    end

    return path
end
