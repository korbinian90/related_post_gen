using JSON3
using StructTypes
using Dates
using StaticArrays

# warmup is done by hyperfine

function relatedIO()
    json_string = read("../posts.json", String)
    posts = JSON3.read(json_string, Vector{PostData})    

    related(posts)

    start = now()
    all_related_posts = related(posts)
    println("Processing time (w/o IO): $(now() - start)")


    open("../related_posts_julia.json", "w") do f
        JSON3.write(f, all_related_posts)
    end
end

struct PostData
    _id::String
    title::String
    tags::Vector{String}
end

struct RelatedPost
    _id::String
    tags::Vector{String}
    related::SVector{5,PostData}
end

StructTypes.StructType(::Type{PostData}) = StructTypes.Struct()

function fastmaxindex!(xs::Vector, topn, maxn, maxv)
    maxn .= 1
    maxv .= 0
    top = maxv[1]
    for (i, x) in enumerate(xs)
        if x > top
            maxv[1] = x
            maxn[1] = i
            for j in 2:topn
                if maxv[j-1] > maxv[j]
                    maxv[j-1], maxv[j] = maxv[j], maxv[j-1]
                    maxn[j-1], maxn[j] = maxn[j], maxn[j-1]
                end
            end
            top = maxv[1]
        end
    end

    reverse!(maxn)

    return maxn
end

function fastmaxindex2!(xs::Vector{T}, topn, maxn, maxv) where T
    pq = PQueue{T}(100)
    for (i, x) in enumerate(xs)
        enqueue!(pq, i, 100-x)
    end
    for i in 1:topn
        maxn[i] = dequeue!(pq)
    end
    return maxn
end


function related(posts)
    for T in (UInt8, UInt16, UInt32, UInt64)
        if length(posts) < typemax(T)
            return related(T, posts)
        end
    end
end
function related(::Type{T}, posts) where {T}
    topn = 5
    # key is every possible "tag" used in all posts
    # value is indicies of all "post"s that used this tag
    tagmap = Dict{String,Vector{T}}()
    for (idx, post) in enumerate(posts)
        for tag in post.tags
            tags = get!(() -> T[], tagmap, tag)
            push!(tags, idx)
        end
    end

    relatedposts = Vector{RelatedPost}(undef, length(posts))
    taggedpostcount = Vector{T}(undef, length(posts))

    maxn = MVector{topn,T}(undef)
    maxv = MVector{topn,T}(undef)

    for (i, post) in enumerate(posts)
        taggedpostcount .= 0
        # for each post (`i`-th)
        # and every tag used in the `i`-th post
        # give all related post +1 in `taggedpostcount` shadow vector
        for tag in post.tags
            for idx in tagmap[tag]
                taggedpostcount[idx] += one(T)
            end
        end

        # don't self count
        taggedpostcount[i] = 0

        fastmaxindex2!(taggedpostcount, topn, maxn, maxv)

        relatedpost = RelatedPost(post._id, post.tags, SVector{topn}(@view posts[maxn]))
        relatedposts[i] = relatedpost
    end

    return relatedposts
end

mutable struct PQueue{T}
    min::Int
    nbins::Int
    content::Vector{Vector{T}}
end

# initialize new queue
PQueue{T}(nbins) where T = PQueue(nbins + 1, nbins, [Vector{T}() for _ in 1:nbins])
PQueue(nbins, item::T, weight=1) where T = enqueue!(PQueue{T}(nbins), item, weight)

Base.isempty(q::PQueue) = q.min > q.nbins

function enqueue!(q::PQueue, item, weight)
    push!(q.content[weight], item)
    q.min = min(q.min, weight)
    return q
end

function dequeue!(q::PQueue)
    elem = pop!(q.content[q.min])
    # increase smallestbin, if elem was last in bin
    while q.min ≤ q.nbins && isempty(q.content[q.min])
        q.min += 1
    end
    return elem
end

const res = relatedIO()
