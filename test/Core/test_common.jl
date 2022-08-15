using AWSS3: s3_put, s3_get
using DataClient:
    FFS,
    FFSMeta,
    S3DB,
    S3DBMeta,
    TIMEZONES,
    decode_type,
    encode_type,
    generate_s3_file_key,
    get_metadata,
    get_s3_file_timestamp,
    s3_cached_get,
    unix2zdt,
    utc_day_floor,
    write_metadata
using Dates
using JSON
using TimeZones
using TimeZones: zdt2unix
using WeakRefStrings

@testset "test src/common.jl" begin
    @testset "test get_metadata S3DB" begin
        coll, ds = "caiso", "test_dataset"
        store = S3DB("test-bucket", "test-prefix")
        expected = S3DBMeta(coll, ds, store, TIMEZONES[coll])
        @test get_metadata(coll, ds, store) == expected
    end

    @testset "test get_metadata FFS" begin
        # basic test
        apply(@patch s3_get(bucket::String, key::String) = read(get_test_data(key))) do
            coll, ds = "caiso", "test_dataset"
            store = FFS("test-bucket", "test-prefix")
            expected = FFSMeta(;
                collection=coll,
                dataset=ds,
                store=store,
                column_order=[
                    "target_start",
                    "target_end",
                    "target_bounds",
                    "release_date",
                    "region",
                    "load",
                    "tag",
                ],
                column_types=Dict(
                    "target_start" => ZonedDateTime,
                    "target_end" => ZonedDateTime,
                    "target_bounds" => Integer,
                    "release_date" => ZonedDateTime,
                    "region" => AbstractString,
                    "load" => AbstractFloat,
                    "tag" => AbstractString,
                ),
                timezone=tz"America/New_York",
                last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
            )
            evaluated = get_metadata(coll, ds, store)
            @test evaluated.collection == expected.collection
            @test evaluated.dataset == expected.dataset
            @test evaluated.column_order == expected.column_order
            @test evaluated.column_types == expected.column_types
            @test evaluated.timezone == expected.timezone
        end

        # test metadata key error from S3, a MissingDataError should be thrown
        apply(@patch s3_get(bucket::String, key::String) = throw(AwsKeyErr)) do
            coll, ds = "caiso", "test_dataset"
            store = FFS("test-bucket", "test-prefix")
            @test_throws MissingDataError get_metadata(coll, ds, store)
        end

        # test metadata non-key error S3, the original error should be thrown
        apply(@patch function s3_get(bucket::String, key::String)
            return get_test_data("missing", AwsOtherErr)
        end) do
            coll, ds = "caiso", "test_dataset"
            store = FFS("test-bucket", "test-prefix")
            @test_throws AwsOtherErr get_metadata(coll, ds, store)
        end
    end

    @testset "test write_metadata FFS" begin
        DATA = Ref{String}()
        patched_s3_put = @patch function s3_put(bucket, key, data)
            DATA[] = data
            return nothing
        end
        apply(patched_s3_put) do
            coll, ds = "caiso", "test_dataset"
            store = FFS("test-bucket", "test-prefix")
            metadata = FFSMeta(;
                collection=coll,
                dataset=ds,
                store=store,
                column_order=[
                    "target_start",
                    "target_end",
                    "target_bounds",
                    "release_date",
                    "region",
                    "load",
                    "tag",
                ],
                column_types=Dict(
                    "target_start" => ZonedDateTime,
                    "target_end" => ZonedDateTime,
                    "target_bounds" => Integer,
                    "release_date" => ZonedDateTime,
                    "region" => AbstractString,
                    "load" => AbstractFloat,
                    "tag" => AbstractString,
                ),
                timezone=tz"America/New_York",
                last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
            )
            expected = JSON.json(
                Dict(
                    "column_order" => metadata.column_order,
                    "column_types" => metadata.column_types,
                    "timezone" => metadata.timezone.name,
                    "last_modified" => 1640995200,
                    "details" => nothing,
                ),
            )
            write_metadata(metadata)
            @test DATA[] == expected
        end
    end

    @testset "test encode/decode types" begin
        expected = Dict(
            String31 => "AbstractString",
            String => "AbstractString",
            Float64 => "AbstractFloat",
            Float32 => "AbstractFloat",
            Int64 => "Integer",
            Int32 => "Integer",
            UInt64 => "Integer",
            ZonedDateTime => "ZonedDateTime",
            DateTime => "DateTime",
            Bool => "Bool",
            Char => "Char",
        )

        for (data_type, str) in pairs(expected)
            @test encode_type(data_type) == str
        end

        expected = Dict(
            "AbstractString" => AbstractString,
            "AbstractFloat" => AbstractFloat,
            "Integer" => Integer,
            "ZonedDateTime" => ZonedDateTime,
            "DateTime" => DateTime,
            "Bool" => Bool,
            "Char" => Char,
        )

        for (str, data_type) in pairs(expected)
            @test decode_type(str) == data_type
        end

        # decode error
        unknown = "UnknownType"
        @test_throws ErrorException("Unable to decode custom type '$unknown'.") decode_type(
            unknown
        )
    end

    @testset "test unix2zdt" begin
        # test basic, default tz == UTC
        dt = DateTime(2022, 1, 1, 12, 30)
        evaluated = unix2zdt(dt2unix(dt))
        @test evaluated == ZonedDateTime(dt, tz"UTC"; from_utc=true)
        @test evaluated.timezone == tz"UTC"

        # test custom timezone
        tz = tz"America/New_York"
        dt = DateTime(2022, 1, 1, 12, 30)
        evaluated = unix2zdt(dt2unix(dt), tz)
        @test evaluated == ZonedDateTime(dt, tz; from_utc=true)
        @test evaluated.timezone == tz

        # Test conversion error due to UnhandledTimeError, falls back to UTC
        # By default, TimeZones.jl only supports a DateTime that can be represented by an Int32
        # https://juliatime.github.io/TimeZones.jl/stable/faq/#future_tzs-1
        tz = tz"America/New_York"
        dt = DateTime(2040, 1, 1)

        evaluated = @test_log(
            DC_LOGGER,
            "warn",
            "Unable to localize '$dt' (UTC) as '$tz', falling back to 'UTC'.",
            unix2zdt(dt2unix(dt), tz),
        )

        # evaluated = unix2zdt(dt2unix(dt), tz)
        @test evaluated == ZonedDateTime(dt, tz"UTC"; from_utc=true)
        @test evaluated.timezone == tz"UTC"
    end

    @testset "test utc_day_floor" begin
        @test utc_day_floor(ZonedDateTime(2022, 1, 5, 23, tz"UTC")) ==
            ZonedDateTime(2022, 1, 5, tz"UTC")
        @test utc_day_floor(ZonedDateTime(2022, 1, 5, 1, tz"UTC")) ==
            ZonedDateTime(2022, 1, 5, tz"UTC")
        @test utc_day_floor(ZonedDateTime(2022, 1, 5, 1, tz"UTC+4")) ==
            ZonedDateTime(2022, 1, 4, tz"UTC")
        @test utc_day_floor(ZonedDateTime(2022, 1, 5, 23, tz"UTC-4")) ==
            ZonedDateTime(2022, 1, 6, tz"UTC")
    end

    @testset "test get_s3_file_timestamp" begin
        @test get_s3_file_timestamp("bucket/prefix1/prefix2/123456789.csv.gz") == 123456789
        @test get_s3_file_timestamp("bucket/prefix1/123456789.csv.gz") == 123456789
        @test get_s3_file_timestamp("bucket/123456789.csv.gz") == 123456789
    end

    @testset "test generate_s3_file_key" begin
        st = S3DB("bucket1", "p1/p2")
        zdt = ZonedDateTime(2022, 1, 5, 23, tz"UTC-2")
        ts = zdt2unix(Int, utc_day_floor(zdt))
        @test generate_s3_file_key("c1", "d1", zdt, st) ==
            "p1/p2/c1/d1/year=2022/$ts.csv.gz"
        # test no prefix
        st = S3DB("bucket1", "")
        zdt = ZonedDateTime(2022, 1, 5, 23, tz"UTC-2")
        ts = zdt2unix(Int, utc_day_floor(zdt))
        @test generate_s3_file_key("c1", "d1", zdt, st) == "c1/d1/year=2022/$ts.csv.gz"
    end

    @testset "test exceptions message format" begin
        error_msg = "error message"

        buff = IOBuffer()
        showerror(buff, ConfigFileError(error_msg))
        @test String(take!(buff)) == "ConfigFileError: $error_msg"

        buff = IOBuffer()
        showerror(buff, DataFrameError(error_msg))
        @test String(take!(buff)) == "DataFrameError: $error_msg"
    end
end
