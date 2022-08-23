using Memento
using Memento.TestUtils: @test_log
using Mocking
using Test

using DataClient

DC_LOGGER = getlogger(DataClient)

Mocking.activate()

@testset "DataClient" begin
    include("helpers.jl")
    include("AWSUtils/runtests.jl")
    include("Core/test_common.jl")
    include("Core/test_configs.jl")
    include("Core/test_list.jl")
    include("Core/test_gather.jl")
    include("Core/test_insert.jl")
    include("Core/test_gather_wrapper.jl")
end
