current = dirname(@__FILE__)
src_dir = joinpath(dirname(current), "src")
unshift!(LOAD_PATH, Pkg.dir())
tmp_pkg = mktempdir()
ENV["JULIA_PKGDIR"] = tmp_pkg
unshift!(LOAD_PATH, src_dir)
for dir in readdir(current)
    full = joinpath(current, dir)
    if ! isdir(full)
        continue
    end
    if ! isempty(ARGS)
        if ! in(dir, ARGS)
            continue
        end
    end
    for fn in readdir(full)
        if ! startswith(fn, "test_")
            continue
        end
        path = joinpath(full, fn)
        println(path)
        include(path)
    end
end
rm(tmp_pkg; recursive=true)
