using AWSS3: s3_put, s3_get
using DataClient:
    FFS,
    FFSMeta,
    Index,
    PartitionSize,
    S3DB,
    S3DBMeta,
    TIMEZONES,
    create_partitions,
    decode,
    decode_type,
    encode,
    encode_type,
    filter_df,
    gen_s3_file_key,
    gen_s3_file_keys,
    get_metadata,
    s3_cached_get,
    sanitize_type,
    unix2zdt,
    value,
    write_metadata
using DataFrames
using Dates
using JSON
using TimeZones
using TimeZones: zdt2unix
using WeakRefStrings

@testset "test src/common.jl" begin
    @testset "test enums" begin
        @test PartitionSize("HOUR") == HOUR
        @test PartitionSize("DAY") == DAY
        @test PartitionSize("MONTH") == MONTH
        @test PartitionSize("YEAR") == YEAR

        @test value(HOUR) == Hour(1)
        @test value(DAY) == Day(1)
        @test value(MONTH) == Month(1)
        @test value(YEAR) == Year(1)
    end

    @testset "test encode/decode index" begin
        @test encode(TimeSeriesIndex("my_key", HOUR)) == Dict(
            "_type" => "TimeSeriesIndex",
            "_attr" => Dict("key" => "my_key", "partition_size" => "HOUR"),
        )

        tsi = TimeSeriesIndex("my_key", HOUR)
        @test decode(Index, encode(tsi)) == tsi

        tsi = TimeSeriesIndex("my_key_2", DAY)
        @test decode(Index, encode(tsi)) == tsi

        tsi = TimeSeriesIndex("my_key_3", MONTH)
        @test decode(Index, encode(tsi)) == tsi

        tsi = TimeSeriesIndex("my_key_4", YEAR)
        @test decode(Index, encode(tsi)) == tsi
    end

    @testset "test get_metadata S3DB" begin
        # basic test
        apply(@patch s3_cached_get(bucket::String, key::String) = get_test_data(key)) do
            coll, ds = "caiso", "dayahead_price"
            store = S3DB("test-bucket", "test-s3db")
            expected = S3DBMeta(
                coll,
                ds,
                store,
                TIMEZONES[coll],
                Dict{String,Any}(
                    "type_map" => Dict{String,Any}(
                        "mlc" => "float",
                        "target_end" => "int",
                        "tag" => "str",
                        "lmp" => "float",
                        "mcc" => "float",
                        "release_date" => "int",
                        "target_bounds" => "int",
                        "target_start" => "int",
                        "node_name" => "str",
                    ),
                    "superkey" => Any[
                        "release_date", "target_start", "target_end", "node_name", "tag"
                    ],
                    "value_key" => Any["lmp", "mlc", "mcc"],
                    "tags" => Dict{String,Any}(
                        "day_ahead_oasis_lmp" => Dict{String,Any}(
                            "time_zone" => "America/Los_Angeles",
                            "content_offset" => 172800,
                            "content_interval" => 86400,
                            "publish_offset" => 43200,
                            "datafeed_runtime" => "None",
                            "publish_interval" => 86400,
                        ),
                    ),
                ),
            )
            evaluated = get_metadata(coll, ds, store)
            @test evaluated.collection == expected.collection
            @test evaluated.dataset == expected.dataset
            @test evaluated.timezone == expected.timezone
            @test evaluated.meta == expected.meta
        end
    end

    @testset "test get_metadata FFS" begin
        # basic test
        apply(@patch s3_cached_get(bucket::String, key::String) = get_test_data(key)) do
            coll, ds = "test-coll", "test-ds"
            store = FFS("test-bucket", "my-prefix")
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
                index=TimeSeriesIndex("target_start", DAY),
                file_format=FileFormats.CSV,
                compression=FileFormats.GZ,
                last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
            )
            evaluated = get_metadata(coll, ds, store)
            @test evaluated.collection == expected.collection
            @test evaluated.dataset == expected.dataset
            @test evaluated.column_order == expected.column_order
            @test evaluated.column_types == expected.column_types
            @test evaluated.index == expected.index
            @test evaluated.file_format == expected.file_format
            @test evaluated.compression == expected.compression
            @test evaluated.timezone == expected.timezone
        end

        # test metadata key error from S3, a MissingDataError should be thrown
        apply(@patch s3_cached_get(bucket::String, key::String) = throw(AwsKeyErr)) do
            coll, ds = "caiso", "test_dataset"
            store = FFS("test-bucket", "test-prefix")
            @test_throws MissingDataError get_metadata(coll, ds, store)
        end

        # test metadata non-key error S3, the original error should be thrown
        apply(@patch s3_cached_get(bucket::String, key::String) = throw(AwsOtherErr)) do
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
                index=TimeSeriesIndex("my_key", DAY),
                file_format=FileFormats.CSV,
                compression=FileFormats.GZ,
                last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
            )
            expected = JSON.json(
                Dict(
                    "column_order" => metadata.column_order,
                    "column_types" => metadata.column_types,
                    "timezone" => metadata.timezone.name,
                    "index" => Dict(
                        "_type" => "TimeSeriesIndex",
                        "_attr" => Dict("key" => "my_key", "partition_size" => "DAY"),
                    ),
                    "file_format" => "CSV",
                    "compression" => "GZ",
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
            AbstractString => "AbstractString",
            AbstractFloat => "AbstractFloat",
            Integer => "Integer",
            ZonedDateTime => "ZonedDateTime",
            DateTime => "DateTime",
            Date => "Date",
            Bool => "Bool",
            Char => "Char",
            String => "String",
            Float64 => "Float64",
            Float32 => "Float32",
            Int64 => "Int64",
            Int32 => "Int32",
            UInt64 => "UInt64",
            Union{Missing,Integer} => ["Union", "Missing", "Integer"],
            Union{Missing,Int64,Int32} => ["Union", "Missing", "Int32", "Int64"],
            Vector{Int32} => ["Array", "Int32", 1],
            Array{Int32,1} => ["Array", "Int32", 1],  # vector
            Array{Int32,2} => ["Array", "Int32", 2],  # matrix
            Array{Int32,3} => ["Array", "Int32", 3],  # 3d array
            Vector{Union{Missing,Int64}} => ["Array", ["Union", "Missing", "Int64"], 1],
            Union{Vector{Int64},Int64} => ["Union", ["Array", "Int64", 1], "Int64"],
            Vector => ["ParametricArray", "Any", 1],
            Array{T,1} where {T} => ["ParametricArray", "Any", 1],
            Array{T,2} where {T<:Int64} => ["ParametricArray", "Int64", 2],
            Array{T,3} where {T<:Integer} => ["ParametricArray", "Integer", 3],
            Array{T,1} where {T<:Union{Missing,Integer}} =>
                ["ParametricArray", ["Union", "Missing", "Integer"], 1],
        )

        for (data_type, str) in pairs(expected)
            @test decode_type(str) == data_type
            @test decode_type(encode_type(data_type)) == data_type
        end

        # encode error
        struct CustomType end
        @test_throws ErrorException("Custom encoding failed for type '$CustomType'.") encode_type(
            CustomType
        )

        # decode error
        unknown = "UnknownType"
        @test_throws ErrorException("Custom decoding failed for type '$unknown'.") decode_type(
            unknown
        )
        unknown2 = ["NotUnion", "Missing", "Integer"]
        @test_throws ErrorException("Custom decoding failed for type '$unknown2'.") decode_type(
            unknown2
        )
    end

    @testset "test sanitize types" begin
        expected = Dict(
            String31 => AbstractString,
            String => AbstractString,
            Float64 => AbstractFloat,
            Float32 => AbstractFloat,
            Int64 => Integer,
            Int32 => Integer,
            UInt64 => Integer,
            ZonedDateTime => ZonedDateTime,
            DateTime => DateTime,
            Date => Date,
            Bool => Bool,
            Char => Char,
            Union{String,Int64,Int32} => Union{AbstractString,Integer},
            Union{Missing,Int64,Int32} => Union{Missing,Integer},
            Vector{Int32} => Vector{T} where {T<:Integer},
            Vector{Float64} => Vector{T} where {T<:AbstractFloat},
            Vector{T} where {T<:String} => Vector{T} where {T<:AbstractString},
            Union{Vector{Int64},Missing,Int32} =>
                Union{Vector{T} where T<:Integer,Missing,Integer},
        )

        for (data_type, sanitized) in pairs(expected)
            @test sanitize_type(data_type) == sanitized
        end
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

    @testset "test TimeSeriesIndex functions" begin
        dt2unix(dt) = convert(Int, datetime2unix(dt))
        function gen_meta(index, fmt=FileFormats.CSV, com=FileFormats.GZ)
            return FFSMeta(;  # helper func to generate test metadata
                collection="coll",
                dataset="ds",
                store=FFS("bucket", "prefix"),
                column_order=["target_start"],
                column_types=Dict("target_start" => ZonedDateTime),
                timezone=tz"America/New_York",
                index=index,
                file_format=fmt,
                compression=com,
                last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
            )
        end

        @testset "test gen_s3_file_key" begin
            dt = DateTime(2022, 3, 4, 5, 15)  # test dt
            ext = FileFormats.extension(FileFormats.CSV, FileFormats.GZ)

            # Test HOUR partition
            meta = gen_meta(TimeSeriesIndex("my_key", HOUR))
            zdt = ZonedDateTime(dt, tz"UTC")
            expected = dt2unix(DateTime(2022, 3, 4, 5))  # hour-floored
            @test gen_s3_file_key(zdt, meta) == "prefix/coll/ds/year=2022/$expected$ext"

            # HOUR partition but non-utc, the hash func should converts it to utc
            zdt = ZonedDateTime(dt, tz"UTC+6")
            expected = dt2unix(DateTime(2022, 3, 3, 23))  # utc-ed then hour-floored
            @test gen_s3_file_key(zdt, meta) == "prefix/coll/ds/year=2022/$expected$ext"

            # Test DAY partition
            meta = gen_meta(TimeSeriesIndex("my_key", DAY))
            expected = dt2unix(DateTime(2022, 3, 3))  # day-floored
            @test gen_s3_file_key(zdt, meta) == "prefix/coll/ds/year=2022/$expected$ext"

            # Test MONTH partition
            meta = gen_meta(TimeSeriesIndex("my_key", MONTH))
            expected = dt2unix(DateTime(2022, 3, 1))  # Month-floored
            @test gen_s3_file_key(zdt, meta) == "prefix/coll/ds/year=2022/$expected$ext"

            # Test YEAR partition
            meta = gen_meta(TimeSeriesIndex("my_key", YEAR))
            expected = dt2unix(DateTime(2022, 1, 1))  # Year-floored
            @test gen_s3_file_key(zdt, meta) == "prefix/coll/ds/year=2022/$expected$ext"
        end

        @testset "test gen_s3_file_keys" begin
            start = ZonedDateTime(2019, 12, 22, 5, 15, tz"UTC+10")
            stop = ZonedDateTime(2022, 1, 4, 22, 1, tz"UTC-4")
            start_utc = DateTime(astimezone(start, tz"UTC"))
            stop_utc = DateTime(astimezone(stop, tz"UTC"))
            ext = FileFormats.extension(FileFormats.CSV, FileFormats.GZ)

            # Test HOUR partition
            meta = gen_meta(TimeSeriesIndex("my_key", HOUR))
            hashes = gen_s3_file_keys(start, stop, meta)
            expected = [
                "prefix/coll/ds/year=$(Dates.year(dt))/$(dt2unix(dt))$ext" for
                dt in floor(start_utc, Hour):Hour(1):floor(stop_utc, Hour)
            ]
            @test expected == hashes

            # Test DAY partition
            meta = gen_meta(TimeSeriesIndex("my_key", DAY))
            hashes = gen_s3_file_keys(start, stop, meta)
            expected = [
                "prefix/coll/ds/year=$(Dates.year(dt))/$(dt2unix(dt))$ext" for
                dt in floor(start_utc, Day):Day(1):floor(stop_utc, Day)
            ]
            @test expected == hashes

            # Test MONTH partition
            meta = gen_meta(TimeSeriesIndex("my_key", MONTH))
            hashes = gen_s3_file_keys(start, stop, meta)
            expected = [
                "prefix/coll/ds/year=$(Dates.year(dt))/$(dt2unix(dt))$ext" for
                dt in floor(start_utc, Month):Month(1):floor(stop_utc, Month)
            ]
            @test expected == hashes

            # Test YEAR partition
            meta = gen_meta(TimeSeriesIndex("my_key", YEAR))
            hashes = gen_s3_file_keys(start, stop, meta)
            expected = [
                "prefix/coll/ds/year=$(Dates.year(dt))/$(dt2unix(dt))$ext" for
                dt in floor(start_utc, Year):Year(1):floor(stop_utc, Year)
            ]
            @test expected == hashes
        end

        @testset "test filter_df" begin
            test_dfs = Dict()
            meta = gen_meta(TimeSeriesIndex("ts", DAY))

            # generate test data, i.e. a map of s3_key -> loaded df
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            for _start in range(start; length=5, step=Day(1))
                key = gen_s3_file_key(_start, meta)
                _stop = _start + Hour(23)
                test_dfs[key] = DataFrame(; ts=collect(_start:Hour(1):_stop))
            end

            # the query range for the test
            query_start = ZonedDateTime(2020, 1, 1, 16, tz"UTC+10")
            query_stop = ZonedDateTime(2020, 1, 3, 21, tz"UTC-1")
            expected_zdts = Set(collect(query_start:Hour(1):query_stop))

            for (key, df) in pairs(test_dfs)
                df = filter_df(df, query_start, query_stop, meta; s3_key=key)
                # show that data is filtered out correctly
                @test isempty(df) || isempty(setdiff(df.ts, expected_zdts))
                test_dfs[key] = df
            end
            queried_data = vcat(values(test_dfs)...)
            # show that data there are no gaps in queried data
            @test isempty(setdiff(expected_zdts, queried_data.ts))
        end
        @testset "test create_partitions" begin
            start = ZonedDateTime(2020, 1, 1, 23, tz"UTC-6")
            stop = ZonedDateTime(2020, 1, 5, 5, tz"UTC+6")
            test_df = DataFrame(; ts=collect(start:Hour(1):stop))

            meta = gen_meta(TimeSeriesIndex("ts", DAY))

            parts = Dict()
            for part in create_partitions(test_df, meta)
                zdts = collect(part.ts)
                parts[zdts[1]] = zdts
            end

            # test each partition, notice that zdts are always converted into UTC first
            # before partitioning 
            part_1_end = ZonedDateTime(2020, 1, 2, 23, tz"UTC")
            @test parts[start] == collect(start:Hour(1):part_1_end)

            part_2_start = ZonedDateTime(2020, 1, 3, tz"UTC")
            part_2_end = part_2_start + Hour(23)
            @test parts[part_2_start] == collect(part_2_start:Hour(1):part_2_end)

            part_3_start = ZonedDateTime(2020, 1, 4, tz"UTC")
            @test parts[part_3_start] == collect(part_3_start:Hour(1):stop)
        end
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
