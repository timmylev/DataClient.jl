using DataClient: FileFormats
using DataClient.FileFormats: Compression, FileFormat, detect_format, extension

@testset "test src/FileFormats/FileFormats.jl" begin
    @testset "test decode" begin
        @test Compression("BZ2") == FileFormats.BZ2
        @test Compression("GZ") == FileFormats.GZ
        @test Compression("LZ4") == FileFormats.LZ4
        @test Compression("ZST") == FileFormats.ZST

        @test FileFormat("ARROW") == FileFormats.ARROW
        @test FileFormat("CSV") == FileFormats.CSV
        @test FileFormat("PARQUET") == FileFormats.PARQUET
    end

    @testset "test extension" begin
        @test extension(FileFormats.ARROW, FileFormats.BZ2) == ".arrow.bz2"
        @test extension(FileFormats.ARROW, nothing) == ".arrow"
        @test extension(FileFormats.ARROW) == ".arrow"
        @test extension(FileFormats.BZ2) == ".bz2"
    end

    @testset "test detect_format" begin
        @test detect_format("dir/file1.csv") == (FileFormats.CSV, nothing)
        @test detect_format("dir/file1.csv.gz") == (FileFormats.CSV, FileFormats.GZ)
        @test detect_format("dir/file1.gz") == (nothing, FileFormats.GZ)
        @test detect_format("dir/file1") == (nothing, nothing)
        @test detect_format("dir/file1.unknown.gz") == (nothing, FileFormats.GZ)
        # unless a compression is known, we assume that the file extension is at the end
        # so in this case, we don't know either.
        @test detect_format("dir/file1.csv.unknown") == (nothing, nothing)

        # double extension error
        @test_throws ErrorException(
            "File dir/file1.csv.gz.gz has double compression extensions '.gz.gz'"
        ) detect_format("dir/file1.csv.gz.gz")
    end
end
