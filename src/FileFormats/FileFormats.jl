module FileFormats

using CodecBzip2
using CodecLz4
using CodecZlib
using CodecZstd
using TranscodingStreams

export COMPRESSION_CODEC, DECOMPRESSION_CODEC
export Compression, FileFormat

"""
    Compression

Support compressions:
- BZ2
- GZ
- LZ4
- ZST
"""
@enum Compression BZ2 GZ LZ4 ZST
# A lookup dict to convert a string back into an enum
const _COMPRESSION = Dict(string.(instances(Compression)) .=> instances(Compression))
Compression(str::AbstractString)::Compression = _COMPRESSION[str]

"""
    FileFormat

Support file formats:
- ARROW
- CSV
- PARQUET
"""
@enum FileFormat ARROW CSV PARQUET
# A lookup dict to convert a string back into an enum
const _FILE_FORMAT = Dict(string.(instances(FileFormat)) .=> instances(FileFormat))
FileFormat(str::AbstractString)::FileFormat = _FILE_FORMAT[str]

const _EXTENSIONS = Dict(
    BZ2 => ".bz2",
    GZ => ".gz",
    LZ4 => ".lz4",
    ZST => ".zst",
    CSV => ".csv",
    ARROW => ".arrow",
    PARQUET => ".parquet",
)

# inverse lookup of _EXTENSIONS
const _EXTENSIONS_INV = Dict(v .=> k for (k, v) in pairs(_EXTENSIONS))

const COMPRESSION_CODEC = Dict(
    BZ2 => Bzip2Compressor,
    GZ => GzipCompressor,
    LZ4 => LZ4FrameCompressor,
    ZST => ZstdCompressor,
)

const DECOMPRESSION_CODEC = Dict(
    BZ2 => Bzip2Decompressor,
    GZ => GzipDecompressor,
    LZ4 => LZ4FrameDecompressor,
    ZST => ZstdDecompressor,
)

extension(key::Union{Compression,FileFormat}) = _EXTENSIONS[key]
extension(fmt::FileFormat, com::Nothing) = extension(fmt)
extension(fmt::FileFormat, com::Compression) = extension(fmt) * extension(com)

function detect_format(
    filename::AbstractString
)::Tuple{Union{FileFormat,Nothing},Union{Compression,Nothing}}
    prefix_1, ext_1 = splitext(filename)
    ext_1 = lowercase(ext_1)

    return if haskey(_EXTENSIONS_INV, ext_1)
        if isa(_EXTENSIONS_INV[ext_1], Compression)
            prefix_2, ext_2 = splitext(prefix_1)
            ext_2 = lowercase(ext_2)

            if haskey(_EXTENSIONS_INV, ext_2)
                if isa(_EXTENSIONS_INV[ext_2], FileFormat)
                    _EXTENSIONS_INV[ext_2], _EXTENSIONS_INV[ext_1]
                else
                    throw(
                        ErrorException(
                            "File $filename has double compression extensions '$ext_2$ext_1'",
                        ),
                    )
                end
            else
                # unknown file format, known compression
                nothing, _EXTENSIONS_INV[ext_1]
            end
        else
            # known file format, no compression
            _EXTENSIONS_INV[ext_1], nothing
        end
    else
        # unknown file format, unknown compression
        nothing, nothing
    end
end

end # module
