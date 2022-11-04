using DataClient.AWSUtils: s3_list_dirs

@testset "test src/list.jl" begin
    test_config_path = abspath(
        joinpath(@__DIR__, "..", "files", "configs", "configs_list.yaml")
    )
    reload_backend(test_config_path)

    px = get_backend()["teststore"].prefix

    mocked_resp = Dict(
        "$px" => ["coll_1", "coll_2", "coll_3"],
        "$(px)coll_1/" => ["ds_1", "ds_2", "ds_3"],
        "$(px)coll_2/" => ["ds_4", "ds_5", "ds_6"],
        "$(px)coll_3/" => ["ds_7", "ds_8", "ds_9"],
    )

    patched_s3_list_dirs = @patch function s3_list_dirs(bucket, prefix)
        return mocked_resp[prefix]
    end

    @testset "test list_datasets" begin
        apply(patched_s3_list_dirs) do
            @test list_datasets() == Dict(
                "teststore" => Dict(
                    "coll_1" => ["ds_1", "ds_2", "ds_3"],
                    "coll_2" => ["ds_4", "ds_5", "ds_6"],
                    "coll_3" => ["ds_7", "ds_8", "ds_9"],
                ),
            )
            @test list_datasets("teststore") == Dict(
                "coll_1" => ["ds_1", "ds_2", "ds_3"],
                "coll_2" => ["ds_4", "ds_5", "ds_6"],
                "coll_3" => ["ds_7", "ds_8", "ds_9"],
            )
            @test list_datasets("teststore", "coll_1") == ["ds_1", "ds_2", "ds_3"]
        end
    end
end
