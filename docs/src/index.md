# DataClient.jl
[DataClient.jl](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl) allows users to store and retrieve processed datasets in the form of a `DataFrame` to and from multiple remote backend locations, just like a database.
Example use cases:
1. On-demand querying of S3DB data directly from DataFeeds (by-passes EISDB).
2. Storing new arbitrary datasets to custom S3 locations.
3. Inserting more data into existing datasets in custom S3 locations.
4. On-demand querying of created datasets in custom S3 locations.

Note: The DataClient does not handle AWS identity and access management (IAM), so any relevant access must be obtained beforehand.

## Contents
```@contents
Pages = [
    "man/getting-started.md",
    "man/configs.md",
    "man/demos.md",
]
Depth = 2
```
```@contents
Pages = [
    "api/api.md"
]
Depth = 3
```

## Index

```@index
Pages = ["api/api.md"]
```