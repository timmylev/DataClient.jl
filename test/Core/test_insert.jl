using AWSS3: s3_get, s3_put
using CSV
using DataClient:
    FFS,
    FFSMeta,
    _ensure_created,
    _merge,
    _process_dataframe!,
    _validate,
    get_s3_file_timestamp,
    get_metadata,
    unix2zdt,
    write_metadata
using DataFrames
using TimeZones

@testset "test src/insert.jl" begin
    dummy_ffs = FFS("bucket", "prefix")

    @testset "test _validate" begin
        df = DataFrame()
        @test_throws DataFrameError("Dataframe must not be empty.") _validate(df, dummy_ffs)

        df = DataFrame(; a=[1, 2, 3], b=[4, 5, 6])
        @test_throws DataFrameError("Missing required column `target_start` for insert.") _validate(
            df, dummy_ffs
        )

        df = DataFrame(; target_start=[1, 2, 3], b=[4, 5, 6])
        tp = eltype(df.target_start)
        @test_throws DataFrameError(
            "Column `target_start` must be a ZonedDateTime, found $tp."
        ) _validate(df, dummy_ffs)
    end

    @testset "test _ensure_created" begin
        freezed_now = now(tz"UTC")

        patched_write = @patch write_metadata(meta) = nothing
        patched_now = @patch now(tz::TimeZone) = freezed_now

        coll = "collection"
        ds = "dataset"
        tz = tz"America/New_York"
        column_order = ["target_start", "target_end", "val_a", "val_b"]
        column_types = Dict(
            "target_start" => ZonedDateTime,
            "target_end" => ZonedDateTime,
            "val_a" => Integer,
            "val_b" => AbstractString,
        )

        test_df = DataFrame(
            "target_start" => [ZonedDateTime(2020, 1, 1, 1, tz)],
            "target_end" => [ZonedDateTime(2020, 1, 1, 2, tz)],
            "val_a" => Vector{Int64}([1]),
            "val_b" => Vector{String}(["abc"]),
        )

        function gen_metadata(required_cols=column_order; details=nothing)
            return FFSMeta(;
                collection=coll,
                dataset=ds,
                store=dummy_ffs,
                column_order=required_cols,
                column_types=column_types,
                timezone=tz,
                last_modified=freezed_now,
                details=details,
            )
        end

        expected = gen_metadata()

        @testset "test new_dataset" begin
            patched_get = @patch get_metadata(args...) = throw(MissingDataError(coll, ds))

            apply([patched_get, patched_write, patched_now]) do
                evaluated = Memento.setlevel!(DC_LOGGER, "debug") do
                    @test_log(
                        DC_LOGGER,
                        "debug",
                        "Metadata for '$coll-$ds' does not exist, creating metadata...",
                        _ensure_created(coll, ds, test_df, dummy_ffs, nothing),
                    )
                end

                @test evaluated.collection == expected.collection
                @test evaluated.dataset == expected.dataset
                @test evaluated.column_order == expected.column_order
                @test evaluated.column_types == expected.column_types
                @test evaluated.timezone == expected.timezone
            end
        end

        @testset "test existing_dataset: valid" begin
            patched_get = @patch get_metadata(args...) = expected

            apply([patched_get, patched_write, patched_now]) do
                evaluated = Memento.setlevel!(DC_LOGGER, "debug") do
                    @test_log(
                        DC_LOGGER,
                        "debug",
                        "Updating existing metadata for dataset '$coll-$ds'.",
                        _ensure_created(coll, ds, test_df, dummy_ffs, nothing),
                    )
                end
            end
        end

        @testset "test existing_dataset: required cols missing" begin
            required_cols = [column_order..., "new_column"]
            patched_get = @patch get_metadata(args...) = gen_metadata(required_cols)

            apply([patched_get, patched_write, patched_now]) do
                @test_throws DataFrameError(
                    "Missing required columns [\"new_column\"] for existing dataset '$coll-$ds'.",
                ) _ensure_created(coll, ds, test_df, dummy_ffs, nothing)
            end
        end

        @testset "test existing_dataset: extra cols found" begin
            required_cols = [column_order[1:(end - 1)]...]
            patched_get = @patch get_metadata(args...) = gen_metadata(required_cols)

            apply([patched_get, patched_write]) do
                @test_log(
                    DC_LOGGER,
                    "warn",
                    "Extra columns [\"val_b\"] found in the input DataFrame for " *
                        "existing dataset '$coll-$ds' will be ignored.",
                    _ensure_created(coll, ds, test_df, dummy_ffs, nothing),
                )
            end
        end

        @testset "test existing_dataset: modified column types" begin
            patched_get = @patch get_metadata(args...) = expected

            apply([patched_get, patched_write]) do
                # test using a DF with incompatible column types
                df = copy(test_df)
                df.val_a = unix2zdt.(df[!, :val_a])
                @test_throws DataFrameError(
                    "The input DataFrame column 'val_a' has type 'ZonedDateTime' " *
                    "which is incompatible with the existing stored type of 'Integer'",
                ) _ensure_created(coll, ds, df, dummy_ffs, nothing)

                # test using a DF with different but still compatible column types
                df = copy(test_df)
                df.val_a = convert.(UInt8, df[!, :val_a])
                # no error is thrown
                _ensure_created(coll, ds, df, dummy_ffs, nothing)
            end
        end

        @testset "test existing_dataset: modify dataset details" begin
            # tracks the argumnent to mocked `write_metadata` function
            CALLED_WITH = Dict{String,Any}()
            patch_write = @patch function write_metadata(metadata)
                return CALLED_WITH["metadata"] = metadata
            end

            # existing details of the dataset
            old_details = Dict("k1" => "old", "k2" => "old")
            patch_get = @patch get_metadata(args...) = gen_metadata(; details=old_details)

            apply([patch_write, patch_get]) do
                to_update = Dict("k2" => "new", "k3" => "new")
                expected = Dict("k1" => "old", "k2" => "new", "k3" => "new")

                _ensure_created(coll, ds, test_df, dummy_ffs, to_update)

                @test CALLED_WITH["metadata"].details == expected
            end
        end
    end

    @testset "test _merge" begin
        tz = tz"America/New_York"
        metadata = FFSMeta(;
            collection="test-coll",
            dataset="test-ds",
            store=FFS("test-bucket", "test-prefix"),
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
            timezone=tz,
            last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
        )

        STORED = Ref{DataFrame}()
        patched_s3_put = @patch function s3_put(bucket, key, data)
            df = DataFrame(CSV.File(IOBuffer(data)))
            _process_dataframe!(df, metadata)
            STORED[] = df
            return nothing
        end

        @testset "test _merge: new file" begin
            patched_s3_get = @patch s3_get(bucket, key) = throw(AwsKeyErr)

            apply([patched_s3_get, patched_s3_put]) do
                s3_key = "1577836800.csv.gz"
                input_df = DataFrame(
                    "target_start" => [ZonedDateTime(2020, 1, 1, 1, tz)],
                    "target_end" => [ZonedDateTime(2020, 1, 1, 2, tz)],
                    "target_bounds" => [1],
                    "release_date" => [ZonedDateTime(2020, 1, 1, 3, tz)],
                    "region" => ["region_A"],
                    "load" => [123.4],
                    "tag" => ["tag_A"],
                )
                _merge(input_df, s3_key, metadata)
                @test STORED[] == input_df
            end
        end

        @testset "test _merge: existing file" begin
            patched_s3_get = @patch s3_get(bucket, key) = get_test_data(key)

            apply([patched_s3_get, patched_s3_put]) do
                s3_key = "1577836800.csv.gz"
                existing_data = DataFrame(CSV.File(get_test_data(s3_key)))
                _process_dataframe!(existing_data, metadata)

                # test that a new row is added
                input_df = DataFrame(
                    "target_start" => [ZonedDateTime(2020, 1, 1, 1, tz)],
                    "target_end" => [ZonedDateTime(2020, 1, 1, 2, tz)],
                    "target_bounds" => [1],
                    "release_date" => [ZonedDateTime(2020, 1, 1, 3, tz)],
                    "region" => ["region_A"],
                    "load" => [123.4],
                    "tag" => ["tag_A"],
                )
                _merge(input_df, s3_key, metadata)
                @test nrow(STORED[]) == nrow(existing_data) + nrow(input_df)

                # re-insert the existing data and show that duplicates are removed
                _merge(existing_data, s3_key, metadata)
                @test nrow(STORED[]) == nrow(existing_data)
            end
        end

        @testset "test _merge: remove extra columns" begin
            patched_s3_get = @patch s3_get(bucket, key) = get_test_data(key)

            apply([patched_s3_get, patched_s3_put]) do
                s3_key = "1577836800.csv.gz"

                # test that a new row is added
                input_df = DataFrame(
                    "target_start" => [ZonedDateTime(2020, 1, 1, 1, tz)],
                    "target_end" => [ZonedDateTime(2020, 1, 1, 2, tz)],
                    "target_bounds" => [1],
                    "release_date" => [ZonedDateTime(2020, 1, 1, 3, tz)],
                    "region" => ["region_A"],
                    "load" => [123.4],
                    "tag" => ["tag_A"],
                    "extra_column" => ["some-val"],
                )
                _merge(input_df, s3_key, metadata)
                @test !("extra_column" in names(STORED[]))
            end
        end
    end

    @testset "test insert FFS" begin
        CALL_TRACKER = Dict{String,DataFrame}()
        patched_merge = @patch function _merge(df, s3key, metadata)
            CALL_TRACKER[s3key] = df
            return nothing
        end
        patched_ensure = @patch _ensure_created(args...) = nothing

        apply([patched_merge, patched_ensure]) do
            # Note that this input df spans 3 days, so it should be partitined into 3
            # files and _merge should be called (only) 3 times.
            # Also note that the df is not sorted, _insert() will handle such cases.
            input_df = DataFrame(
                "target_start" => [
                    ZonedDateTime(2020, 1, 1, 1, tz"UTC"),
                    ZonedDateTime(2020, 1, 2, 1, tz"UTC"),
                    ZonedDateTime(2020, 1, 3, 1, tz"UTC"),
                    ZonedDateTime(2020, 1, 1, 2, tz"UTC"),
                    ZonedDateTime(2020, 1, 2, 2, tz"UTC"),
                    ZonedDateTime(2020, 1, 3, 2, tz"UTC"),
                ],
            )

            expected = Dict{Int,Vector{ZonedDateTime}}(
                1577836800 => [
                    ZonedDateTime(2020, 1, 1, 1, tz"UTC"),
                    ZonedDateTime(2020, 1, 1, 2, tz"UTC"),
                ],
                1577923200 => [
                    ZonedDateTime(2020, 1, 2, 1, tz"UTC"),
                    ZonedDateTime(2020, 1, 2, 2, tz"UTC"),
                ],
                1578009600 => [
                    ZonedDateTime(2020, 1, 3, 1, tz"UTC"),
                    ZonedDateTime(2020, 1, 3, 2, tz"UTC"),
                ],
            )

            reload_configs(
                joinpath(@__DIR__, "..", "files", "configs", "configs_valid.yaml")
            )
            insert("test-coll", "test-ds", input_df, "myffs")

            for (s3key, df) in pairs(CALL_TRACKER)
                ts = get_s3_file_timestamp(s3key)
                @test expected[ts] == df.target_start
            end
        end
    end
end
