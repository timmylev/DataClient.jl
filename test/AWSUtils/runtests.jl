using AWSS3: s3_get
using DataClient.AWSUtils: FileCache, s3_cached_get, s3_list_dirs
using DataClient.AWSUtils.S3: list_objects_v2
using DataClient.Configs: reload_configs
using TranscodingStreams: transcode

@testset "test src/AWSUtils/s3_cached_get.jl" begin
    cfg_path = joinpath(pwd(), "configs.yaml")
    reload_configs(cfg_path)

    FILE_SIZE_MB = 2
    CALL_COUNTER = Ref(0)
    patched_s3_get = @patch function s3_get(s3_bucket, s3_key; kwargs...)
        CALL_COUNTER[] += 1
        return zeros(UInt8, FILE_SIZE_MB * 1000000)
    end

    TRANSCODED_COUNT = Ref(0)
    patched_transcode = @patch function transcode(codec, data)
        TRANSCODED_COUNT[] += 1
        return data
    end

    @testset "test using default cache" begin
        apply(patched_s3_get) do
            CALL_COUNTER[] = 0
            # downloading 'file_1.txt' for the first time, s3_get() is called
            stream = s3_cached_get("test-bucket-1", "file_1.txt")
            @test CALL_COUNTER[] == 1
            # 'file_1.txt' is cached, s3_get() is not called
            stream = s3_cached_get("test-bucket-1", "file_1.txt")
            @test CALL_COUNTER[] == 1
            # downloading 'file_2.txt' for the first time, s3_get() is called
            stream = s3_cached_get("test-bucket-1", "file_2.txt")
            @test CALL_COUNTER[] == 2
            # downloading 'file_1.txt' from a different bucket, s3_get() is called
            stream = s3_cached_get("test-bucket-2", "file_1.txt")
            @test CALL_COUNTER[] == 3
        end
    end

    @testset "test using custom cache with size limit" begin
        # The cache will hold 3 files max
        cache = FileCache(FILE_SIZE_MB * 3)

        apply(patched_s3_get) do
            CALL_COUNTER[] = 0
            # download and cache 'file_1.txt'
            stream = s3_cached_get("test-bucket", "file_1.txt", cache)
            @test CALL_COUNTER[] == 1
            # verify that 'file_1.txt' is cached
            stream = s3_cached_get("test-bucket", "file_1.txt", cache)
            @test CALL_COUNTER[] == 1

            # download and cache 'file_2.txt'
            stream = s3_cached_get("test-bucket", "file_2.txt", cache)
            @test CALL_COUNTER[] == 2
            # verify that 'file_1.txt' and 'file_2.txt' are cached
            stream = s3_cached_get("test-bucket", "file_1.txt", cache)
            stream = s3_cached_get("test-bucket", "file_2.txt", cache)
            @test CALL_COUNTER[] == 2

            # download and cache 'file_3.txt'
            stream = s3_cached_get("test-bucket", "file_3.txt", cache)
            @test CALL_COUNTER[] == 3
            # verify that 'file_1.txt', 'file_2.txt', and 'file_3.txt' are cached
            stream = s3_cached_get("test-bucket", "file_1.txt", cache)
            stream = s3_cached_get("test-bucket", "file_2.txt", cache)
            stream = s3_cached_get("test-bucket", "file_3.txt", cache)
            @test CALL_COUNTER[] == 3

            # download and cache 'file_4.txt',
            # this will invalidate 'file_1.txt' because the max cache size is reached
            stream = s3_cached_get("test-bucket", "file_4.txt", cache)
            @test CALL_COUNTER[] == 4
            # verify that 'file_2.txt', 'file_3.txt', and 'file_4.txt' are cached
            stream = s3_cached_get("test-bucket", "file_2.txt", cache)
            stream = s3_cached_get("test-bucket", "file_3.txt", cache)
            stream = s3_cached_get("test-bucket", "file_4.txt", cache)
            @test CALL_COUNTER[] == 4
            # and 'file_1.txt' is no longer cached and has been deleted
            @test isfile(joinpath(cache.dir, "test-bucket", "file_4.txt"))  # exist
            @test !isfile(joinpath(cache.dir, "test-bucket", "file_1.txt"))  # not exist

            # getting 'file_1.txt' results in a re-download
            stream = s3_cached_get("test-bucket", "file_1.txt", cache)
            @test CALL_COUNTER[] == 5
        end
    end

    @testset "test instantiate persistent cache" begin
        apply(patched_s3_get) do
            # Unset the global cache
            DataClient.AWSUtils.unset_global_cache()
            @test DataClient.AWSUtils._DEFAULT_CACHE[] == nothing

            # set the ENV and reload configs
            cache_dir = mktempdir()
            withenv(
                "DATACLIENT_CACHE_DIR" => cache_dir,
                "DATACLIENT_CACHE_SIZE_MB" => "200",
                "DATACLIENT_CACHE_EXPIRE_AFTER_DAYS" => "5",
            ) do
                @test length(ls_R(cache_dir)) == 0  # new dir is empty
                reload_configs(cfg_path)

                # files will now be cached
                stream = s3_cached_get("test-bucket-1", "file_1.txt")
                @test length(ls_R(cache_dir)) == 1

                # When the cache is reinstantiated, the previously cached files will persist
                DataClient.AWSUtils.unset_global_cache()
                stream = s3_cached_get("test-bucket-1", "file_2.txt")
                @test length(ls_R(cache_dir)) == 2  # incremented

                # test that cache removes stale files by speeding up time by 10 days
                curr_time = now()
                apply([patched_s3_get, @patch now() = curr_time + Day(10)]) do
                    # Now reset the cache, remember that we've set the cache config with
                    # a 5-day expiry previously
                    DataClient.AWSUtils.unset_global_cache()
                    stream = s3_cached_get("test-bucket-1", "file_4.txt")
                    @test length(ls_R(cache_dir)) == 1  # resets to 1
                end
            end
        end
    end

    @testset "test decompress cached files" begin
        apply([patched_s3_get, patched_transcode]) do
            # non-compressed file (based on extension)
            cached_path = s3_cached_get("test-bucket-2", "file_1.csv")
            @test TRANSCODED_COUNT[] == 0

            # compressed file (based on extension)
            cached_path = s3_cached_get("test-bucket-2", "file_2.csv.gz")
            @test TRANSCODED_COUNT[] == 1
            @test endswith(cached_path, ".csv")

            cached_path = s3_cached_get("test-bucket-2", "file_3.arrow.GZ")
            @test TRANSCODED_COUNT[] == 2
            @test endswith(cached_path, ".arrow")

            cached_path = s3_cached_get("test-bucket-2", "file_4.arrow.ZST")
            @test TRANSCODED_COUNT[] == 3
            @test endswith(cached_path, ".arrow")

            # disable decompression
            cached_path = s3_cached_get(
                "test-bucket-2", "file_5.arrow.ZST"; decompress=false
            )
            @test TRANSCODED_COUNT[] == 3
            @test endswith(cached_path, ".arrow.ZST")
        end
    end
end

@testset "test src/AWSUtils/s3_list_dirs.jl" begin
    patched_list_objects_v2 = @patch function list_objects_v2(bucket, args)
        return Dict(
            "CommonPrefixes" => [
                Dict("Prefix" => "prefix_1/prefix_2/prefix_3/"),
                Dict("Prefix" => "prefix_1/prefix_2/prefix_4/"),
                Dict("Prefix" => "prefix_1/prefix_2/prefix_5/"),
            ],
        )
    end

    @testset "test s3_list_dirs" begin
        apply(patched_list_objects_v2) do
            dirs = s3_list_dirs("bucket", "prefix"; full_path=false)
            @test collect(dirs) == ["prefix_3", "prefix_4", "prefix_5"]

            dirs = s3_list_dirs("bucket", "prefix"; full_path=true)
            @test collect(dirs) == [
                "prefix_1/prefix_2/prefix_3/",
                "prefix_1/prefix_2/prefix_4/",
                "prefix_1/prefix_2/prefix_5/",
            ]
        end
    end
end
