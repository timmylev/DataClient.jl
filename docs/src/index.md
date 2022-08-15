# DataClient.jl
[DataClient.jl](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl) allows users to store and retrieve processed datasets in the form of a `DataFrame` to and from multiple remote data warehouses.
This includes storing derived datasets in custom S3 buckets and retrieving datasets from centralized S3 buckets such as S3DB (where EISDB gets its data from).

Note: The DataClient does not handle identity and access management (IAM), so any relevant access must be obtained beforehand.

## Contents
```@contents
Pages = [
    "man/getting-started.md",
    "man/configs.md",
    "man/demos.md",
    "man/developer-notes.md",
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