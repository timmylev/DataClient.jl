"""
    s3_list_dirs(
        bucket::String, parent_dir::String=""; full_path::Bool=false
    )::Channel{String}

Lists a single level (non-recursive) of "sub-directories" in an S3 bucket.

!!!note
    Sub-directories don't actually exist in an S3 bucket, they are simply S3 Key
    prefixes delimitered by "/".

# Arguments
- `bucket`: The S3 bucket
- `parent_dir`: (Optional) A parent dir to list from, defaults to the root dir.

# Keywords
- `full_path`: (Optional) If true, returns the full path (from root) of the sub-dirs.
"""
function s3_list_dirs(
    bucket::String, parent_dir::String=""; full_path::Bool=false
)::Channel{String}
    return Channel(; ctype=String, csize=128) do chnl
        args = Dict(
            "prefix" => joinpath(parent_dir, ""),  # appends a "/" if not present
            "delimiter" => "/",
        )

        while true
            r = @mock S3.list_objects_v2(bucket, args)

            if haskey(r, "CommonPrefixes")
                # ensure that it is an array
                ref = r["CommonPrefixes"]
                cps = isa(ref, Vector) ? ref : [ref]

                for object in cps
                    dir_path = object["Prefix"]
                    # if full_path == false, grab the last dir in the path,
                    # for example. extract 'dir_4' in 'dir_1/dir_2/dir_3/dir_4/'
                    dir = full_path ? dir_path : rsplit(dir_path, "/"; limit=3)[end - 1]
                    put!(chnl, dir)
                end
            end

            # paginate
            args["continuation-token"] = get(r, "NextContinuationToken", "")
            isempty(args["continuation-token"]) && break
        end
    end
end
