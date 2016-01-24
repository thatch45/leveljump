import LevelJump

function test_basic()
    path = mktempdir()
    ldb = LevelJump.LevelDB(path)
    LevelJump.push!(ldb, "cheese", "yes")
end

test_basic()