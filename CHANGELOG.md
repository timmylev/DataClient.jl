# Changelog

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
