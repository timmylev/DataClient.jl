using Documenter, DataClient

makedocs(;
    modules=[DataClient],
    format=Documenter.HTML(; prettyurls=false),
    pages=[
        "Home" => "index.md",
        "Manual" => [
            "Getting Started" => "man/getting-started.md",
            "Configs and Backend" => "man/configs.md",
            "Demos" => "man/demos.md",
            "Gotchas" => "man/gotchas.md",
            "Developer Notes" => "man/developer-notes.md",
        ],
        "API" => "api/api.md",
    ],
    repo="https://gitlab.invenia.ca/invenia/Datafeeds/DataClient.jl/blob/{commit}{path}#{line}",
    sitename="DataClient.jl",
    authors="Invenia Labs",
    strict=true,
    checkdocs=:exports,
)
