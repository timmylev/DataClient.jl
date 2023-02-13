using DataClient: load_df, write_df
using DataFrames
using TimeZones

ZDT(args...) = ZonedDateTime(args..., tz"America/Los_Angeles")
DT(args...) = DateTime(args...)

@testset "test src/formats.jl" begin
    @testset "test arrow" begin
        df = DataFrame(;
            target_start=[ZDT(2020, 1, 1, 1), ZDT(2020, 1, 1, 2), ZDT(2020, 1, 1, 3)],
            dt=[DT(2020, 1, 1, 1), DT(2020, 1, 1, 2), DT(2020, 1, 1, 3)],
            region=["region_A", "region_B", "region_C"],
            string_pairs=[["key1, val1"], ["key2, val2"], ["key3, val3"]],
            int_vals=[1, 2, 3],
            float_vals=[123.4, 456.7, 789.0],
            float_lists=[[1.2, 3.4], [5.6, 7.8], [9.8]],
            bool=[true, false, false],
        )

        fmt = FileFormats.ARROW
        for com in [instances(FileFormats.Compression)..., nothing]
            @test df == load_df(write_df(df, fmt, com), fmt, com)
        end
    end

    @testset "test parquet" begin
        df = DataFrame(;
            dt=[missing, DT(2020, 1, 1, 1), DT(2020, 1, 1, 2), DT(2020, 1, 1, 3)],
            region=["region_A", "region_B", "region_C", "region_D"],
            string_pairs=[missing, ["key1, val1"], ["key2, val2"], ["key3, val3"]],
            int_vals=[missing, 1, 2, 3],
            float_vals=[123.4, 456.7, 789.0, 889.4],
            float_lists=[[1.2, 3.4], [5.6, 7.8], [9.8], [9.8, 8.5, 223.98]],
            bool=[missing, true, false, false],
        )

        fmt = FileFormats.PARQUET
        for com in [instances(FileFormats.Compression)..., nothing]
            @test isequal(df, load_df(write_df(df, fmt, com), fmt, com))
        end
    end

    @testset "test csv: non-vector" begin
        df = DataFrame(;
            target_start=[1667433600, 1667347200, 1667260800],
            region=["region_A", "region_B", "region_C"],
            int_vals=[1, 2, 3],
            float_vals=[123.4, 456.7, 789.0],
            bool=[true, false, false],
        )

        fmt = FileFormats.CSV
        for com in [instances(FileFormats.Compression)..., nothing]
            @test df == load_df(write_df(df, fmt, com), fmt, com)
        end
    end

    @testset "test csv: vector" begin
        df = DataFrame(; string_pairs=[["key1, val1"], ["key2, val2"], ["key3, val3"]])

        fmt = FileFormats.CSV
        com = nothing
        # CSV.jl does not support vector types. This is fine becuase we plan on using
        # ARROW format for FFS datsets, we may support CSV vector types in FFS on a
        # future date. Note that this is only relevent for FFS, loading S3DB CSV vector
        # types works fine becuase the vector conversion is done separately outside of
        # the load_df() function.
        @test df != load_df(write_df(df, fmt, com), fmt, com)
    end
end
