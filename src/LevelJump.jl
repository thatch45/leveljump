module LevelJump

const lib = "libleveldb"

type LevelDB
    db::Ptr
    #it::Ptr
    path::AbstractString
end

function LevelDB(path::AbstractString, create_if_missing::Bool=true)
    options = ccall( (:leveldb_options_create, lib), Ptr{Void}, ())
    if create_if_missing
        ccall(
              (:leveldb_options_set_create_if_missing, lib),
              Void,
              (Ptr{Void}, UInt8),
              options, 1)
    end
    err = Ptr{UInt8}[0]
    db = ccall(
               (:leveldb_open, lib),
               Ptr{Void},
               (Ptr{Void}, Ptr{UInt8}, Ptr{Ptr{UInt8}}),
               options,
               path,
               err)
    if db == C_NULL
        error(bytestring(err[1]))
    end
    #it = ccall(
    #           (:leveldb_create_iterator, lib),
    #           Ptr{Void},
    #           (Ptr{Void}, Ptr{Void}),
    #           db, options,
    #           )
    ldb = LevelDB(db, path)
    return ldb
end

function open(path::AbstractString, create_if_missing::Bool=true)
    return LevelDB(path, create_if_missing)
end

function close(ldb::LevelDB)
    ccall(
          (:leveldb_close, lib),
          Void,
          (Ptr{Void},),
          ldb.db)
end

function push!(ldb::LevelDB, key::AbstractString, val::AbstractString)
    options = ccall(
                    (:leveldb_writeoptions_create, lib),
                    Ptr{Void},
                    ())
    err = Ptr{UInt8}[0]
    ccall(
          (:leveldb_put, lib),
          Void,
          (Ptr{Void}, Ptr{Void}, Cstring, UInt, Cstring, UInt, Ptr{Ptr{UInt8}}),
          ldb.db, options, key, length(key), val, length(val), err,
          )
    if err[1] != C_NULL
        error(bytestring(err[1]))
    end
end

function setindex!(ldb::LevelDB, key::AbstractString, val::AbstractString)
    push!(ldb, key, val)
end

function get(ldb::LevelDB, key::AbstractString, default)
    return getindex(ldb, key)
end

function getintex(ldb::LevelDB, key::AbstractString)
    options = ccall( (:leveldb_readoptions_create, lib), Ptr{Void}, ())
    err = Ptr{UInt8}[0]
    val_len = Csize_t[0]
    value = ccall(
                  (:leveldb_get, lib),
                  Cstring,
                  (Ptr{Void}, Ptr{Void}, Cstring, UInt, Ptr{Csize_t}, Ptr{Ptr{UInt8}}),
                  ldb.db, options, key, length(key), val_len, err
                  )
    if err[1] != C_NULL
        error(bytestring(err[1]))
    else
        ret = bytestring(value)
        return ret
    end
end

function delete!(ldb::LevelDB, key::AbstractString)
    options = ccall(
                    (:leveldb_writeoptions_create, lib),
                    Ptr{Void},
                    ()
                    )
    err = Ptr{UInt8}[0]
    ccall(
          (:leveldb_delete, lib),
          Void,
          (Ptr{Void}, Ptr{Void}, Cstring, UInt, Ptr{Ptr{UInt8}} ),
          ldb.db, options, key, length(key), err
          )
    if err[1] !=C_NULL
        error(bytestring(err[1]))
    end
end

function pop!(ldb::LevelDB, key::AbstractString, default::Any=Nothing)
    ret = get(ldb, key, default)
    delete!(ldb, key)
    return ret
end

function batch(ldb::LevelDB, array::Array)
    w_batch = ccall((:leveldb_writebatch_create, lib), Ptr{Void},())
    for set in array
        ccall((:leveldb_writebatch_put, lib), Void,
          (Ptr{UInt8}, Cstring, UInt, Cstring, UInt),
          w_batch, set["key"], length(set["key"]), set["value"], length(set["value"]))
    end
    options = ccall(
                    (:leveldb_writeoptions_create, lib),
                    Ptr{Void},
                    ())
    err = Ptr{UInt8}[0]
    ccall((:leveldb_write, libleveldbjl), Void,
          (Ptr{Void}, Ptr{Void}, Ptr{Void},  Ptr{Ptr{UInt8}} ),
          ldb, options, w_batch, err)
    if err[1] != C_NULL
        error(bytestring(err[1]))
    end
end

function iter_valid(ldb::LevelDB)
    return ccall(
                 (:leveldb_iter_valid, lib),
                 UInt8,
                 (Ptr{Void},),
                 ldb.it) == 1
end

function iter_value(ldb::LevelDB)
    v_len = Csize_t[0]
    value = ccall(
                  (:leveldb_iter_value, libleveldbjl),
                  Ptr{UInt8},
                  (Ptr{Void}, Ptr{Csize_t}),
                  ldb.it, v_len)
    return pointer_to_array(value, (v_len[1],), false)
end

function iter_key(ldb::LevelDB)
    k_len = Csize_t[0]
    key = ccall((:leveldb_iter_key, lib),
                Cstring,
                (Ptr{Void}, Ptr{Csize_t}),
                ldb.it, k_len)
    return bytestring(key, k_len[1])
end

function iter_seek(ldb::LevelDB, key::AbstractString)
    ccall(
          (:leveldb_iter_seek, lib),
          Void,
          (Ptr{Void}, Cstring, UInt),
          ldb.it, key, length(key))
end

function iter_next(ldb::LevelDB)
    ccall(
          (:leveldb_iter_next, lib),
          Void,
          (Ptr{Void},),
          ldb.it)
end

# module end
end
