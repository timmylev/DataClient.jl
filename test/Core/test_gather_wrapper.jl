using DataClient: FFSMeta, S3DB, _get_zdt_cols, get_metadata
using DataFrames
using Dates
using TimeZones
using TimeZones: zdt2unix

@testset "test src/gather_wrapper.jl" begin
    # load in the test configs
    test_config_path = abspath(
        joinpath(@__DIR__, "..", "files", "configs", "configs_gather.yaml")
    )
    reload_configs(test_config_path)

    STORE_ID = "teststore"

    # patch gather (ZDT-based)
    CALLED_WITH = Dict{String,Any}()  # tracks arguments to mocked functions
    base_gather = @patch function gather(
        collection::AbstractString,
        dataset::AbstractString,
        start_dt::ZonedDateTime,
        end_dt::ZonedDateTime,
        store_id::AbstractString,
    )
        CALLED_WITH["start_dt"] = start_dt
        CALLED_WITH["end_dt"] = end_dt
        # mocked data
        zdts = [
            ZonedDateTime(2020, 1, 1, 1, tz"UTC"),
            ZonedDateTime(2020, 1, 1, 2, tz"UTC"),
            ZonedDateTime(2020, 1, 1, 3, tz"UTC"),
        ]
        bounds = [1 for zdt in zdts]
        return DataFrame(
            "release_date" => zdts,
            "target_start" => zdts,
            "target_end" => zdts,
            "target_bounds" => bounds,
            "tag" => ["tag_a" for zdt in zdts],
        )
    end

    # patch get_metadata
    TZ = tz"America/New_York"
    get_meta = @patch function get_metadata(coll, ds, store)
        return FFSMeta(;
            collection=coll,
            dataset=ds,
            store=store,
            column_order=[
                "target_start", "target_end", "target_bounds", "release_date", "tag"
            ],
            column_types=Dict(
                "target_start" => ZonedDateTime,
                "target_end" => ZonedDateTime,
                "target_bounds" => Integer,
                "release_date" => ZonedDateTime,
                "tag" => AbstractString,
            ),
            timezone=TZ,
            last_modified=ZonedDateTime(2022, 1, 1, tz"UTC"),
        )
    end

    @testset "test gather: Date" begin
        apply([base_gather, get_meta]) do
            gather("coll", "ds", Date(2020, 1, 1), STORE_ID)
            # test that the `Date` is correctly converted into a start/end ZDT
            @test CALLED_WITH["start_dt"] == ZonedDateTime(2020, 1, 1, TZ)
            @test CALLED_WITH["end_dt"] == ZonedDateTime(2020, 1, 1, 23, 59, 59, TZ)
        end
    end

    @testset "test gather: DateTime" begin
        apply([base_gather, get_meta]) do
            gather("coll", "ds", DateTime(2020, 1, 1), DateTime(2020, 12, 1), STORE_ID)
            # test that the `DateTime`s are correctly converted into a start/end ZDT
            @test CALLED_WITH["start_dt"] == ZonedDateTime(2020, 1, 1, TZ)
            @test CALLED_WITH["end_dt"] == ZonedDateTime(2020, 12, 1, TZ)
        end
    end

    @testset "test gather: missing dataset" begin
        get_meta_error = @patch function get_metadata(coll, ds, store)
            throw(MissingDataError(coll, ds))
        end
        apply([base_gather, get_meta_error]) do
            @test_throws MissingDataError("coll", "ds") gather(
                "coll", "ds", DateTime(2020, 1, 1), DateTime(2020, 12, 1), STORE_ID
            )
        end
    end

    @testset "test gather: strip tz" begin
        apply([base_gather, get_meta]) do
            df = gather("coll", "ds", Date(2020, 1, 1), STORE_ID; strip_tz=true)
            @test eltype(df.release_date) == DateTime
            @test eltype(df.target_start) == DateTime
            @test eltype(df.target_end) == DateTime

            df = gather("coll", "ds", Date(2020, 1, 1), STORE_ID; strip_tz=false)
            @test eltype(df.release_date) == ZonedDateTime
            @test eltype(df.target_start) == ZonedDateTime
            @test eltype(df.target_end) == ZonedDateTime
        end
    end

    @testset "test _get_zdt_cols" begin
        store = get_metadata("miso", "day_ahead_lmp", S3DB("bucket", "prefix"))
        # These are hard coded constants, simply testing for 100% coverage
        @test _get_zdt_cols(store) == ["target_start", "target_end", "release_date"]
    end
end
