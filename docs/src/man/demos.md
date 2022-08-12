# Demos

## Demo 1: Using DataClient.jl to Store and Retrieve Processed Datasets from DataSoupParsers.jl
In this demo, we'll be parsing the dataset `MISOData.Day_Ahead_Cleared_Offers` from DataSoupParsers.jl and storing the processed dataset in `s3://tim-buck/test-insert/`.

### Step 1: Configuring the Storage Location
Create the config file to specify the custom storage location.
```sh
cat > configs.yaml <<- EOF
additional-stores:
  - tim-personal: ffs:s3://tim-buck/test-insert/
EOF
```
See [Configs and Backend](@ref) for more details, but basically, `tim-personal` can be replaced with any meaningful identifier and replace the S3 URI with your desired storage location (do not remove the `ffs:`).

### Step 2: Gathering Source Data using DataSoupParsers.jl
Refer to [DataSoupParsers.jl](https://invenia.pages.invenia.ca/research/DataSoupParsers.jl/) for more info.
```julia
julia> using DataSoupParsers
julia> using Dates

julia> dataset = MISOData.Day_Ahead_Cleared_Offers
julia> start_date = Date(2020, 4, 1)
julia> end_date = Date(2020, 4, 30)

# This is slow: Downloads and parses unprocessed data files.
# Also, notice that gather may return multiple sub-datasets.
julia> da_offers = gather(dataset, start_date, end_date)
Dict{String, DataFrames.DataFrame} with 4 entries:
  "Hourly_Data" => 881664×14 DataFrame…
  "Bid_Curve"   => 3746820×5 DataFrame…
  "Cleared_MW"  => 909024×3 DataFrame…
  "Daily_Data"  => 36736×3 DataFrame…
```

### Step 3: Storing Processed Data using DataClient.jl
We can now store the processed `DataFrame`(s) using DataClient.jl.
```julia
# Both DataSoupParsers and DataClient exports `gather`, so module namespacing will be required.
julia> using DataClient
WARNING: using DataClient.gather in module Main conflicts with an existing identifier.
julia> using TimeZones

# To keep this demo short, we'll only be storing one of the sub-datasets obtained previously.
julia> df = da_offers["Hourly_Data"]
881664×14 DataFrame

# DataClient.jl requires that every dataset contain a `target_start` column of type `ZonedDateTime`.
# This is typically not available for datasets obtained via DataSoupParsers.jl. So, we'll have to
# add it manually.
julia> df_types = Dict(names(df) .=> eltype.(eachcol(df)))
Dict{String, Type} with 14 entries:
  "Unit_Code"               => Int64
  "Must_Run_Flag"           => Int64
  "Target_MW_Reduction"     => Union{Missing, Float64}
  "Emergency_Flag"          => Int64
  "Slope"                   => Int64
  "Economic_Max"            => Float64
  "Mkthour_Begin_EST"       => DateTime
  "Economic_Flag"           => Int64
  "Unit_Available_Flag"     => Int64
  "Curtailment_Offer_Price" => Union{Missing, Float64}
  "Emergency_Max"           => Float64
  "Economic_Min"            => Float64
  "Emergency_Min"           => Float64
  "Self_Scheduled_MW"       => Float64

# Add the `target_start` column of type `ZonedDateTime`
julia> miso_tz = tz"EST"
julia> df.target_start = ZonedDateTime.(df.Mkthour_Begin_EST, miso_tz)

# Now we can store the processed dataset
julia> collection_name = "MisoData"  # any name we want
julia> dataset_name = "Day_Ahead_Cleared_Offers-Hourly_Data"  # any name we want
julia> store_id = "tim-personal"  # identifier used in the configs.yaml file
julia> insert(collection_name, dataset_name, df, store_id)

# Retrieve the processed dataset
julia> start_dt = ZonedDateTime(2020, 4, 1, miso_tz)
julia> end_dt = ZonedDateTime(2020, 5, 1, miso_tz)
# DataClient namespace required because of conflict with DataSoupParsers.gather
julia> processed = DataClient.gather(collection_name, dataset_name, start_dt, end_dt, store_id)
881664×15 DataFrame
```
More data can be inserted into existing datasets as long as their columns match.
DataClient.jl will automatically deduplicate rows to avoid storing the same data twice.
