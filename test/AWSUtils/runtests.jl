using AWSS3: s3_get_file
using DataClient.AWSUtils: FileCache, s3_cached_get, s3_list_dirs
using DataClient.AWSUtils.S3: list_objects_v2

@testset "test src/AWSUtils/s3_cached_get.jl" begin
    FILE_SIZE_MB = 2
    CALL_COUNTER = Ref(0)

    patched_s3_get_file = @patch function s3_get_file(
        s3_bucket, s3_key, file_path; kwargs...
    )
        CALL_COUNTER[] += 1
        stream = IOBuffer(zeros(UInt8, FILE_SIZE_MB * 1000000))
        open(file_path, "w") do file
            while !eof(stream)
                write(file, readavailable(stream))
            end
        end
        return nothing
    end

    @testset "test using default cache" begin
        apply(patched_s3_get_file) do
            CALL_COUNTER[] = 0
            # downloading 'file_1.txt' for the first time, s3_get_file() is called
            stream = s3_cached_get("test-bucket-1", "file_1.txt")
            @test CALL_COUNTER[] == 1
            # 'file_1.txt' is cached, s3_get_file() is not called
            stream = s3_cached_get("test-bucket-1", "file_1.txt")
            @test CALL_COUNTER[] == 1
            # downloading 'file_2.txt' for the first time, s3_get_file() is called
            stream = s3_cached_get("test-bucket-1", "file_2.txt")
            @test CALL_COUNTER[] == 2
            # downloading 'file_1.txt' from a different bucket, s3_get_file() is called
            stream = s3_cached_get("test-bucket-2", "file_1.txt")
            @test CALL_COUNTER[] == 3
        end
    end

    @testset "test using custom cache with size limit" begin
        # The cache will hold 3 files max
        cache = FileCache(FILE_SIZE_MB * 3)

        apply(patched_s3_get_file) do
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
