using AWSS3: s3_list_keys
using DataClient:
    MissingDataError,
    S3DB,
    _find_s3_files,
    _generate_keys,
    _load_s3_files,
    _process_dataframe!,
    unix2zdt,
    zdt2unix
using DataClient.AWSUtils: s3_cached_get
using DataFrames
using Dates
using TimeZones
using TimeZones: zdt2unix

@testset "test src/gather.jl" begin
    # load in the test configs
    test_config_path = abspath(
        joinpath(@__DIR__, "..", "files", "configs", "configs_gather.yaml")
    )
    reload_configs(test_config_path)
    STORE = get_backend()["teststore"]

    @testset "test _load_s3_files" begin
        apply(@patch s3_cached_get(bucket, key) = get_test_data(key)) do
            start_dt = ZonedDateTime(2020, 1, 1, tz"UTC")
            end_dt = ZonedDateTime(2020, 1, 5, tz"UTC")
            # 5 keys will be generated, one for each day
            file_keys = _generate_keys("test-coll", "test-ds", start_dt, end_dt, STORE)
            # test the load function
            df = _load_s3_files(file_keys, "test-coll", "test-ds", start_dt, end_dt, STORE)
            @test !isempty(df)
            # our test data covers the full (hourly) range queried above,
            # show that there are no gaps in the loaded data.
            loaded = Set(df.target_start)
            expected = collect(range(start_dt, end_dt; step=Hour(1)))
            @test isempty(setdiff(expected, loaded))

            # test that the filter is working correctly
            start_dt = ZonedDateTime(2020, 1, 2, 15, tz"UTC-4")
            end_dt = ZonedDateTime(2020, 1, 4, 12, tz"UTC+2")
            file_keys = _generate_keys("test-coll", "test-ds", start_dt, end_dt, STORE)
            df = _load_s3_files(file_keys, "test-coll", "test-ds", start_dt, end_dt, STORE)
            loaded = Set(df.target_start)
            expected = collect(range(start_dt, end_dt; step=Hour(1)))
            # show that we're not loading in any unexpected data
            @test isempty(setdiff(loaded, expected))
        end
    end

    @testset "test _load_s3_files errors" begin
        start_dt = ZonedDateTime(2020, 1, 1, tz"UTC")
        end_dt = ZonedDateTime(2020, 1, 9, tz"UTC")
        file_keys = _generate_keys("test-coll", "test-ds", start_dt, end_dt, STORE)

        # Missing S3 Key Error are caught and not thrown
        apply(@patch s3_cached_get(b, k) = get_test_data(k, AwsKeyErr)) do
            df = _load_s3_files(file_keys, "test-coll", "test-ds", start_dt, end_dt, STORE)
        end

        # Other Errors are thrown
        apply(@patch s3_cached_get(b, k) = get_test_data(k, AwsOtherErr)) do
            @test_throws AwsOtherErr _load_s3_files(
                file_keys, "test-coll", "test-ds", start_dt, end_dt, STORE
            )
        end

        # returns an empty DataFrame if no data is found
        apply(@patch s3_cached_get(b, k) = get_test_data(k, AwsKeyErr)) do
            start_dt = ZonedDateTime(2020, 1, 6, tz"UTC")
            file_keys = _generate_keys("test-coll", "test-ds", start_dt, end_dt, STORE)
            df = _load_s3_files(file_keys, "test-coll", "test-ds", start_dt, end_dt, STORE)
            # returns a empty DataFrame if no data is found
            @test typeof(df) == DataFrame
            @test isempty(df)
        end
    end

    @testset "test _process_dataframe! S3DB" begin
        zdts = [
            ZonedDateTime(2020, 1, 1, 1, tz"UTC"),
            ZonedDateTime(2020, 1, 1, 2, tz"UTC"),
            ZonedDateTime(2020, 1, 1, 3, tz"UTC"),
        ]
        bounds = [1 for zdt in zdts]

        df = DataFrame(
            "release_date" => zdt2unix.(Int, zdts),
            "target_start" => zdt2unix.(Int, zdts),
            "target_end" => zdt2unix.(Int, zdts),
            "target_bounds" => bounds,
            "tag" => ["tag_a" for zdt in zdts],
        )

        coll = "spp"
        ds = "test-ds"
        tz = DataClient.TIMEZONES[coll]
        metadata = DataClient.S3DBMeta(coll, ds, S3DB("buck", "prex"), tz)

        _process_dataframe!(df, metadata)
        # show that columns are reordered
        @test names(df) ==
            ["target_start", "target_end", "target_bounds", "release_date", "tag"]
        # show that zdts are decoded
        @test df.target_start == zdts
        @test df.target_end == zdts
        @test df.release_date == zdts
        # show that bounds are decoded
        @test df.target_bounds == [DataClient.BOUNDS[b] for b in bounds]
        # show that timezones are correct, we use specific timezones for s3db data
        @test timezone(first(df).target_start) == DataClient.TIMEZONES[coll]
        @test timezone(first(df).target_end) == DataClient.TIMEZONES[coll]
        @test timezone(first(df).release_date) == DataClient.TIMEZONES[coll]
    end

    @testset "test _process_dataframe! FFS" begin
        zdts = [
            ZonedDateTime(2020, 1, 1, 1, tz"UTC"),
            ZonedDateTime(2020, 1, 1, 2, tz"UTC"),
            ZonedDateTime(2020, 1, 1, 3, tz"UTC"),
        ]
        df = DataFrame(
            "release_date" => zdt2unix.(Int, zdts),
            "target_start" => zdt2unix.(Int, zdts),
            "target_end" => zdt2unix.(Int, zdts),
        )

        coll = "test-coll"
        ds = "test-ds"
        store = FFS("buck", "prex")
        column_order = names(df)
        column_types = Dict(
            "target_start" => ZonedDateTime, "release_date" => Int64, "target_end" => Int64
        )
        tz = tz"America/New_York"
        metadata = DataClient.FFSMeta(coll, ds, store, column_order, column_types, tz)

        _process_dataframe!(df, metadata)
        # show that only 'target_start' is decoded (because of the column types)
        @test df.target_start == zdts
        @test df.target_end == zdt2unix.(Int, zdts)
        @test df.release_date == zdt2unix.(Int, zdts)
    end

    @testset "test _find_s3_files" begin
        patched_s3_list_keys = @patch function s3_list_keys(bucket, prefix)
            # mock the case where a file exists for every day in the year.
            year = parse(Int, match(r"year=(\d{4})", prefix)[1])
            start = ZonedDateTime(year, 1, 1, tz"UTC")
            stop = ZonedDateTime(year, 12, 31, tz"UTC")
            return _generate_keys("test-coll", "test-ds", start, stop, STORE)
        end

        apply(patched_s3_list_keys) do
            start = ZonedDateTime(2020, 6, 9, tz"UTC")
            stop = ZonedDateTime(2022, 9, 11, tz"UTC")
            keys = _find_s3_files("test-coll", "test-ds", start, stop, STORE)
            # show that _find_s3_files() is correctly filtering out out of range files
            expected = _generate_keys("test-coll", "test-ds", start, stop, STORE)
            @test isempty(setdiff(Set(keys), Set(expected)))
        end
    end

    @testset "test gather" begin
        patched_load = @patch function _load_s3_files(args...)
            return DataFrame(; target_start=[ZonedDateTime(2020, 1, 1, tz"UTC")])
        end

        COUNTER_FIND = Ref(0)
        patched_find = @patch function _find_s3_files(coll, ds, start, stop, store)
            COUNTER_FIND[] += 1
            return Vector{String}()
        end

        COUNTER_GEN = Ref(0)
        patched_gen = @patch function _generate_keys(coll, ds, start, stop, store)
            COUNTER_GEN[] += 1
            return Vector{String}()
        end

        apply([patched_load, patched_find, patched_gen]) do
            # test small range (less than 10 days)
            # `gather` will generate keys directly and skip the 'find' step
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 1, 5, tz"UTC")
            df = gather("test-coll", "test-ds", start, stop)
            @test COUNTER_GEN[] == 1
            @test COUNTER_FIND[] == 0

            # test large range (more than 10 days)
            # `gather` will 'find' instead of generating keys directly
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 2, 1, tz"UTC")
            df = gather("test-coll", "test-ds", start, stop)
            @test COUNTER_GEN[] == 1
            @test COUNTER_FIND[] == 1

            # same thing but calling gather while specifying a store_id
            COUNTER_GEN[] = 0
            COUNTER_FIND[] = 0
            # test small range (less than 10 days)
            # `gather` will generate keys directly and skip the 'find' step
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 1, 5, tz"UTC")
            df = gather("test-coll", "test-ds", start, stop, "teststore")
            @test COUNTER_GEN[] == 1
            @test COUNTER_FIND[] == 0

            # test large range (more than 10 days)
            # `gather` will 'find' instead of generating keys directly
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 2, 1, tz"UTC")
            df = gather("test-coll", "test-ds", start, stop, "teststore")
            @test COUNTER_GEN[] == 1
            @test COUNTER_FIND[] == 1
        end

        patched_load = @patch function _load_s3_files(args...)
            return DataFrame()
        end

        # test missing data, error is thrown
        apply([patched_load, patched_find, patched_gen]) do
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 2, 1, tz"UTC")
            args = ["test-coll", "test-ds", start, stop]
            @test_throws MissingDataError(args...) gather(args...)
            @test_throws MissingDataError(args...) gather(args..., "teststore")
        end
    end
end
