using DataClient: Configs

@testset "test src/Configs/Configs.jl" begin
    test_cfg_prefix = abspath(joinpath(@__DIR__, "..", "files", "configs"))
    test_file_1 = joinpath(test_cfg_prefix, "Configs_Test_1.yaml")
    test_file_2 = joinpath(test_cfg_prefix, "Configs_Test_2.yaml")

    @testset "test reload" begin
        Configs.reload_configs(test_file_1)
        cfg = Configs.get_configs()
        @test cfg["DATACLIENT_CACHE_DIR"] == "/home/user/cache"
        @test cfg["DATACLIENT_CACHE_SIZE_MB"] == 100
        @test cfg["DATACLIENT_CACHE_DECOMPRESS"] == false

        Configs.reload_configs(test_file_2)
        cfg = Configs.get_configs()
        @test cfg["DATACLIENT_CACHE_DIR"] == "/home/user/cache_2"
        @test cfg["DATACLIENT_CACHE_SIZE_MB"] == 200
        @test cfg["DATACLIENT_CACHE_DECOMPRESS"] == true

        # ENV VARs will override config files
        ENV["DATACLIENT_CACHE_SIZE_MB"] = 500
        Configs.reload_configs(test_file_2)
        cfg = Configs.get_configs()
        @test cfg["DATACLIENT_CACHE_DIR"] == "/home/user/cache_2"
        @test cfg["DATACLIENT_CACHE_SIZE_MB"] == 500
        @test cfg["DATACLIENT_CACHE_DECOMPRESS"] == true

        # back to normal
        delete!(ENV, "DATACLIENT_CACHE_SIZE_MB")
        Configs.reload_configs(test_file_2)
        @test Configs.get_configs()["DATACLIENT_CACHE_SIZE_MB"] == 200
    end
end
