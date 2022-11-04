using AWSS3: s3_list_keys, s3_get
using DataClient:
    FFS,
    FFSMeta,
    MissingDataError,
    S3DB,
    _filter_missing,
    _load_s3_files,
    _process_dataframe!,
    get_metadata,
    gen_s3_file_keys,
    unix2zdt,
    zdt2unix
using DataClient.AWSUtils: s3_cached_get
using DataFrames
using Dates
using JSON
using TimeZones
using TimeZones: zdt2unix

@testset "test src/gather.jl" begin
    # load in the test configs
    test_config_path = abspath(
        joinpath(@__DIR__, "..", "files", "configs", "configs_gather.yaml")
    )
    reload_backend(test_config_path)
    STORE = get_backend()["teststore"]
    COLL, DS = "test-coll", "test-ds"

    patched_s3_get = @patch s3_get(bucket::String, key::String) = read(get_test_data(key))
    patched_s3_cached_get = @patch s3_cached_get(b, k; kwargs...) = get_test_data(k)

    # load test data
    METADATA = apply(patched_s3_cached_get) do
        get_metadata(COLL, DS, STORE)
    end

    @testset "test _load_s3_files" begin
        apply(patched_s3_cached_get) do
            start_dt = ZonedDateTime(2020, 1, 1, tz"UTC")
            end_dt = ZonedDateTime(2020, 1, 5, tz"UTC")
            # 5 keys will be generated, one for each day
            file_keys = gen_s3_file_keys(start_dt, end_dt, METADATA)
            # test the load function
            df = _load_s3_files(file_keys, start_dt, end_dt, METADATA)
            @test !isempty(df)
            # our test data covers the full (hourly) range queried above,
            # show that there are no gaps in the loaded data.
            loaded = Set(df[!, METADATA.index.key])
            expected = collect(range(start_dt, end_dt; step=Hour(1)))
            @test isempty(setdiff(expected, loaded))

            # test that the filter is working correctly
            start_dt = ZonedDateTime(2020, 1, 2, 15, tz"UTC-4")
            end_dt = ZonedDateTime(2020, 1, 4, 12, tz"UTC+2")
            file_keys = gen_s3_file_keys(start_dt, end_dt, METADATA)
            df = _load_s3_files(file_keys, start_dt, end_dt, METADATA)
            loaded = Set(df[!, METADATA.index.key])
            expected = collect(range(start_dt, end_dt; step=Hour(1)))
            # show that we're not loading in any unexpected data
            @test isempty(setdiff(loaded, expected))
        end
    end

    @testset "test _load_s3_files errors" begin
        start_dt = ZonedDateTime(2020, 1, 1, tz"UTC")
        end_dt = ZonedDateTime(2020, 1, 9, tz"UTC")

        file_keys = gen_s3_file_keys(start_dt, end_dt, METADATA)

        # Missing S3 Key Error are caught and not thrown
        apply(@patch s3_cached_get(b, k; kwargs...) = get_test_data(k, AwsKeyErr)) do
            df = _load_s3_files(file_keys, start_dt, end_dt, METADATA)
            @test !isempty(df)
        end

        # Other Errors are thrown
        apply(@patch s3_cached_get(b, k; kwargs...) = get_test_data(k, AwsOtherErr)) do
            @test_throws AwsOtherErr _load_s3_files(file_keys, start_dt, end_dt, METADATA)
        end

        # returns an empty DataFrame if no data is found
        apply(@patch s3_cached_get(b, k; kwargs...) = get_test_data(k, AwsKeyErr)) do
            start_dt = ZonedDateTime(2020, 1, 6, tz"UTC")
            file_keys = gen_s3_file_keys(start_dt, end_dt, METADATA)
            df = _load_s3_files(file_keys, start_dt, end_dt, METADATA)
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
        curves = ["[12.4, 5.7, 3.6]", missing, "[1.2, null, 6.3]"]
        mw_blocks = ["[1.0]", "[1.0, 2.0]", "[1.0, 2.0]"]
        bounds = [1 for zdt in zdts]

        df = DataFrame(
            "release_date" => zdt2unix.(Int, zdts),
            "target_start" => zdt2unix.(Int, zdts),
            "target_end" => zdt2unix.(Int, zdts),
            "target_bounds" => bounds,
            "tag" => ["tag_a" for zdt in zdts],
            "off_nonspin_offer_curve" => curves,
            "mw_blocks" => mw_blocks,
            "multi_hour" => [0, 0, 1],
        )

        coll, ds = "datasoup", "ercot_da_gen_ancillary_offers"
        store = S3DB("test-bucket", "test-s3db")
        metadata = apply(patched_s3_cached_get) do
            get_metadata(coll, ds, store)
        end

        _process_dataframe!(df, metadata)
        # show that columns are reordered
        @test names(df) == [
            "target_start",
            "target_end",
            "target_bounds",
            "release_date",
            "off_nonspin_offer_curve",
            "mw_blocks",
            "multi_hour",
            "tag",
        ]
        # show that zdts are decoded
        @test df.target_start == zdts
        @test df.target_end == zdts
        @test df.release_date == zdts
        # show that bounds are decoded
        @test df.target_bounds == [DataClient.BOUNDS[b] for b in bounds]
        # show that list_types are decoded correctly
        @test isequal(
            df.off_nonspin_offer_curve, [[12.4, 5.7, 3.6], missing, [1.2, missing, 6.3]]
        )
        @test isequal(df.mw_blocks, [[1.0], [1.0, 2.0], [1.0, 2.0]])
        @test eltype(df.mw_blocks) <: Vector{<:AbstractFloat}
        # parse bools
        @test df.multi_hour == [false, false, true]
        # show that timezones are correct, we use specific timezones for s3db data
        @test timezone(first(df).target_start) == DataClient.get_tz(coll, ds)
        @test timezone(first(df).target_end) == DataClient.get_tz(coll, ds)
        @test timezone(first(df).release_date) == DataClient.get_tz(coll, ds)
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

        metadata = FFSMeta(;
            collection="test-coll",
            dataset="test-ds",
            store=FFS("buck", "prex"),
            column_order=names(df),
            column_types=Dict(
                "target_start" => ZonedDateTime,
                "release_date" => Int64,
                "target_end" => Int64,
            ),
            timezone=tz"America/New_York",
            index=TimeSeriesIndex("my_key", DAY),
            storage_format=CSV_GZ,
            last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
        )

        _process_dataframe!(df, metadata)
        # show that only 'target_start' is decoded (because of the column types)
        @test df.target_start == zdts
        @test df.target_end == zdt2unix.(Int, zdts)
        @test df.release_date == zdt2unix.(Int, zdts)
    end

    @testset "test _filter_missing" begin
        start = ZonedDateTime(2020, 6, 9, tz"UTC")
        stop = ZonedDateTime(2022, 9, 11, tz"UTC")
        all_keys = gen_s3_file_keys(start, stop, METADATA)

        random_indexes = unique(rand(1:length(all_keys), 100))
        available_keys = getindex(all_keys, random_indexes)

        apply(@patch s3_list_keys(bucket, prefix) = available_keys) do
            found_keys = _filter_missing(all_keys, METADATA)
            @test sort(found_keys) == sort(available_keys)
        end
    end

    @testset "test gather" begin
        patched_load = @patch function _load_s3_files(args...)
            return DataFrame(; target_start=[ZonedDateTime(2020, 1, 1, tz"UTC")])
        end

        COUNTER_FIND = Ref(0)
        patched_find = @patch function _filter_missing(keys, meta)
            COUNTER_FIND[] += 1
            return Vector{String}()
        end

        apply([patched_load, patched_find, patched_s3_cached_get]) do
            # test small range (less than 10 days)
            # `gather` will generate keys directly and skip the 'find' step
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 1, 5, tz"UTC")
            df = gather(COLL, DS, start, stop)
            @test COUNTER_FIND[] == 0

            # test large range (more than 10 days)
            # 'find' will be called instead of just generating keys directly
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 2, 1, tz"UTC")
            df = gather(COLL, DS, start, stop)
            @test COUNTER_FIND[] == 1

            # same thing but calling gather while specifying a store_id
            COUNTER_FIND[] = 0
            # test small range (less than 10 days)
            # `gather` will generate keys directly and skip the 'find' step
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 1, 5, tz"UTC")
            df = gather(COLL, DS, start, stop, "teststore")
            @test COUNTER_FIND[] == 0

            # test large range (more than 10 days)
            # 'find' will be called instead of just generating keys directly
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 2, 1, tz"UTC")
            df = gather(COLL, DS, start, stop, "teststore")
            @test COUNTER_FIND[] == 1
        end

        patched_load = @patch function _load_s3_files(args...)
            return DataFrame()
        end

        # test missing data, error is thrown
        apply([patched_load, patched_find, patched_s3_cached_get]) do
            start = ZonedDateTime(2020, 1, 1, tz"UTC")
            stop = ZonedDateTime(2020, 2, 1, tz"UTC")
            args = [COLL, DS, start, stop]
            @test_throws MissingDataError(args...) gather(args...)
            @test_throws MissingDataError(args...) gather(args..., "teststore")
        end
    end
end
