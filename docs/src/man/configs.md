# Configs and Backend
The config file is a user-defined `.yaml` file that supplies optional configurations to the DataClient.
A sample config file can be found in [DataClient.jl/configs-example.yaml](https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl).

To use one, simply add your configs to a file, name it `configs.yaml`, and place it your current working directory (the `pwd()` of your Julia process).
This is the default load path the DataClient uses to check for custom configs.

## How The File is Loaded
The config file is loaded in automatically when any of the supported operations are called ([`get_backend`](@ref), [`list_datasets`](@ref), [`gather`](@ref), and [`insert`](@ref)).
Once loaded in for the first time, it is cached in memory for future use.
So, making manual changes to the config file will not take immediate effect unless the Julia session is restarted or `reload_configs()` is called.

As mentioned above, the default load path of the config file is `joinpath(pwd(), "configs.yaml")`.
To use a custom path, an explicit [`reload_configs`](@ref) call will be required.

## Configurables

### Adding Additional Stores
The optional `additional-stores` config allows you to add any number of additional stores to the DataClient.
For example:
```yaml
additional-stores:
  - my-ffs: ffs:s3://my-ffs-bucket/
  - my-ffs-2: ffs:s3://my-ffs-bucket-2/my-prefix-1/
  - my-ffs-2b: ffs:s3://my-ffs-bucket-2/my-prefix-2/
  - staging-datafeeds: s3db:s3://datafeeds-staging-bucket/bucket-prefix/
```
Notice that the store URI is prefixed with a storage type and we currently only support two storage types:
- `s3db` : [`DataClient.S3DB`](@ref) is used for storage locations that contain data that is transmuted by Datafeeds. This store type is read-only thus you will not be able to perform [`insert`](@ref) operations. It is very unlikely that you will have to add an additional `s3db` store because we already hard-code the production Datafeeds URI in the package as a centralized store.
- `ffs` : [`DataClient.FFS`](@ref) is used for user-derived datasets where data is typically stored manually using the [`insert`](@ref) operation. Examples of this are personal S3 buckets or some shared S3 bucket that you have been given access to. When setting up your own `ffs`, it is fine to reuse the same bucket for multiple stores but __ensure that the bucket prefix for each store do not overlap.__ Each store must exist within an isolated 'space'.

The order in which you specify the additional stores is important because **this is the same order that the [`gather`](@ref) operation uses to search for datasets** when a store id is not provided.

### Prioritizing Additional Stores over Centralized Stores
If a store id is not provided when using the [`gather`](@ref) operation, the default behavior is to first check any hard-coded centralized stores before checking the user-defined `additional-stores`.
This behavior can be switched around by setting the `prioritize-additional-stores` config.
```yaml
prioritize-additional-stores: True
```

### Disabling Centralized Stores
The optional `disable-centralized` config allows you to disable any hard-coded centralized stores and only use the user-defined `additional-stores`.
```yaml
disable-centralized: True
```