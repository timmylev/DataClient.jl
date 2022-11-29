# Changelog

## Version 1.13.0

### Features
* Support Arrow format for the `S3DB` data
 [!17](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/17)

## Version 1.12.0

### Features
* Support for Arrow files and DataFrame metadata [!16](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/16)

## Version 1.11.0

### Features
* Upgrade Cache to support file decompression and persistent directories [!15](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/15)

## Version 1.10.0

### Features
* Support variable partition sizes and custom columns for `TimeSeriesIndex` [!14](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/14)
* Updated Type encoding/decoding scheme and support Arrays [!14](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/14)


## Version 1.9.0

### Features
* Support parsing S3DB `bool` columns [!12](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/12)

## Version 1.8.0

### Features
* Parse list types into `Vector`s with more specific element types [!11](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/11)

## Version 1.7.0

### Features
* Implement async and add timing logs to `insert` [!10](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/10)


## Version 1.6.0

### Features
* Support loading S3DB vector columns [!9](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/9)


## Version 1.5.1

### Fixes
* Modify the behavior for `AWSUtils.s3_cached_get` to return a file path instead of a file descriptor [!8](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/8)

## Version 1.5.0

### Features
* Support User-defined Column Types When Creating a New Dataset [!7](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/7)

## Version 1.4.0

### Features
* Add support to encode/decode the `Date` type [!6](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/6)

## Version 1.3.0

### Features
[!5](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/5)
* Add timing trace logs for `gather`
* Optimize DataFrame `vcat` in `gather`

## Version 1.2.0

### Features
* TZ-naive `gather()` and Additional Dataset Metadata [!3](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/3)


## Version 1.1.0

### Features
* Support Unions in DataFrame Type Maps [!2](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/-/merge_requests/2)
