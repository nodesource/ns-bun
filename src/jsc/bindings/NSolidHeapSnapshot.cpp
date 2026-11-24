#include "root.h"
#include "NSolidHeapSnapshot.h"
#include <JavaScriptCore/DeferGC.h>
#include <JavaScriptCore/HeapAnalyzer.h>
#include <JavaScriptCore/HeapProfiler.h>
#include <JavaScriptCore/InternalFunction.h>
#include <JavaScriptCore/JSFunction.h>
#include <JavaScriptCore/JSScope.h>
#include <JavaScriptCore/JSString.h>
#include <JavaScriptCore/PreventCollectionScope.h>
#include <JavaScriptCore/RegExp.h>
#include <JavaScriptCore/RegExpObject.h>
#include <wtf/HashMap.h>
#include <wtf/Lock.h>
#include <wtf/text/MakeString.h>
#include <wtf/text/StringBuilder.h>
#include <wtf/text/StringHash.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <optional>

#if OS(LINUX)
#include <fcntl.h>
#include <unistd.h>
#endif

extern "C" uint64_t Bun__Os__getFreeMemory(void);

namespace Bun {

using namespace JSC;

namespace {

// V8's node and edge types, numbered as in the snapshot's meta.
enum class NodeType : uint8_t {
    Hidden,
    Array,
    String,
    Object,
    Code,
    Closure,
    RegExp,
    Number,
    Native,
    Synthetic,
    ConcatenatedString,
    SlicedString,
    Symbol,
    BigInt,
    ObjectShape,
};
enum class EdgeType : uint8_t {
    Context,
    Element,
    Property,
    Internal,
    Hidden,
    Shortcut,
    Weak,
};

constexpr const char* snapshotMeta = "{\"snapshot\":{\"meta\":{"
                                     "\"node_fields\":[\"type\",\"name\",\"id\",\"self_size\",\"edge_count\",\"trace_node_id\",\"detachedness\"],"
                                     "\"node_types\":[[\"hidden\",\"array\",\"string\",\"object\",\"code\",\"closure\",\"regexp\",\"number\",\"native\",\"synthetic\","
                                     "\"concatenated string\",\"sliced string\",\"symbol\",\"bigint\",\"object shape\"],"
                                     "\"string\",\"number\",\"number\",\"number\",\"number\",\"number\"],"
                                     "\"edge_fields\":[\"type\",\"name_or_index\",\"to_node\"],"
                                     "\"edge_types\":[[\"context\",\"element\",\"property\",\"internal\",\"hidden\",\"shortcut\",\"weak\"],\"string_or_number\",\"node\"],"
                                     "\"trace_function_info_fields\":[\"function_id\",\"name\",\"script_name\",\"script_id\",\"line\",\"column\"],"
                                     "\"trace_node_fields\":[\"id\",\"function_info_index\",\"count\",\"size\",\"children\"],"
                                     "\"sample_fields\":[\"timestamp_us\",\"last_assigned_id\"],"
                                     "\"location_fields\":[\"object_index\",\"script_id\",\"line\",\"column\"]},"
                                     "\"node_count\":";
constexpr unsigned nodeFieldCount = 7;
// V8 cuts names, the text of strings included, to this many characters.
constexpr unsigned maxNameLength = 1024;
// What the graph and its JSON take per cell, its edges included: a snapshot
// of 1.2 million cells added 234 bytes per cell to the peak RSS. The text of
// strings, when it is kept, comes on top.
constexpr uint64_t costPerCell = 320;

// An edge as the collector reports it. `from` is null for a root.
struct CellEdge {
    JSCell* from;
    JSCell* to;
    EdgeType type;
    RootMarkReason reason;
    uint32_t index;
    UniquedStringImpl* name;
};

// An edge between nodes, by their indices. `nameOrIndex` is a string's index
// for a named edge, else a number.
struct NodeEdge {
    uint32_t from;
    uint32_t to;
    EdgeType type;
    uint32_t nameOrIndex;
};

struct Node {
    NodeType type;
    uint32_t name;
    uint64_t selfSize;
    uint32_t edgeCount;
};

class StringTable {
public:
    StringTable() { add(emptyString()); }

    uint32_t add(const String& string)
    {
        const String& key = string.isNull() ? emptyString() : string;
        auto result = m_indices.add(key, m_strings.size());
        if (result.isNewEntry)
            m_strings.append(key);
        return result.iterator->value;
    }

    const Vector<String>& strings() const { return m_strings; }

private:
    HashMap<String, uint32_t> m_indices;
    Vector<String> m_strings;
};

class ChunkWriter {
public:
    explicit ChunkWriter(size_t chunkBytes)
        : m_chunkBytes(chunkBytes)
    {
        m_current.reserveInitialCapacity(chunkBytes);
    }

    void append(std::span<const uint8_t> bytes)
    {
        m_current.append(bytes);
        if (m_current.size() >= m_chunkBytes) {
            m_chunks.append(std::exchange(m_current, {}));
            m_current.reserveInitialCapacity(m_chunkBytes);
        }
    }
    void append(const char* text) { append(std::span { reinterpret_cast<const uint8_t*>(text), strlen(text) }); }
    void append(uint64_t number)
    {
        char buffer[24];
        auto result = std::to_chars(buffer, buffer + sizeof buffer, number);
        append(std::span { reinterpret_cast<const uint8_t*>(buffer), static_cast<size_t>(result.ptr - buffer) });
    }
    void appendQuoted(const String& string)
    {
        StringBuilder builder;
        builder.appendQuotedJSONString(string);
        auto utf8 = builder.toString().utf8();
        append(byteCast<uint8_t>(utf8.span()));
    }

    Vector<Vector<uint8_t>> finish()
    {
        if (!m_current.isEmpty())
            m_chunks.append(WTF::move(m_current));
        return WTF::move(m_chunks);
    }

private:
    size_t m_chunkBytes;
    Vector<uint8_t> m_current;
    Vector<Vector<uint8_t>> m_chunks;
};

static String truncatedName(const String& name)
{
    return name.length() > maxNameLength ? name.left(maxNameLength) : name;
}

static String systemName(JSCell* cell)
{
    return makeString("system / "_s, cell->classInfo()->className);
}

// A cell's node type and name, as V8 would give them to the like object.
static std::pair<NodeType, String> describe(VM& vm, JSCell* cell, bool redact)
{
    if (cell->isString()) {
        auto* string = asString(cell);
        if (string->isRope())
            return string->isSubstring() ? std::pair { NodeType::SlicedString, String("(sliced string)"_s) } : std::pair { NodeType::ConcatenatedString, String("(concatenated string)"_s) };
        if (redact)
            return { NodeType::String, String("(redacted)"_s) };
        auto* impl = string->tryGetValueImpl();
        return { NodeType::String, impl ? truncatedName(String(impl)) : emptyString() };
    }
    if (cell->isSymbol())
        return { NodeType::Symbol, String("symbol"_s) };
    if (cell->isHeapBigInt())
        return { NodeType::BigInt, String("bigint"_s) };
    switch (cell->type()) {
    case StructureType:
        return { NodeType::ObjectShape, systemName(cell) };
    case NativeExecutableType:
    case ProgramExecutableType:
    case ModuleProgramExecutableType:
    case EvalExecutableType:
    case FunctionExecutableType:
    case UnlinkedFunctionExecutableType:
    case UnlinkedProgramCodeBlockType:
    case UnlinkedModuleProgramCodeBlockType:
    case UnlinkedEvalCodeBlockType:
    case UnlinkedFunctionCodeBlockType:
    case CodeBlockType:
        return { NodeType::Code, systemName(cell) };
    default:
        break;
    }
    if (!cell->isObject())
        return { NodeType::Hidden, systemName(cell) };
    if (auto* function = dynamicDowncast<JSFunction>(cell))
        return { NodeType::Closure, truncatedName(function->nameWithoutGC(vm)) };
    if (auto* function = dynamicDowncast<InternalFunction>(cell))
        return { NodeType::Closure, truncatedName(function->name()) };
    if (auto* regExp = dynamicDowncast<RegExpObject>(cell))
        return { NodeType::RegExp, redact ? String("(redacted)"_s) : truncatedName(regExp->regExp()->pattern()) };
    if (dynamicDowncast<JSScope>(cell))
        return { NodeType::Hidden, String("system / Context"_s) };
    return { NodeType::Object, truncatedName(JSObject::calculatedClassName(asObject(cell))) };
}

static ASCIILiteral rootName(RootMarkReason reason)
{
    switch (reason) {
#define ROOT_NAME(name)        \
    case RootMarkReason::name: \
        return "(" #name ")"_s;
        FOR_EACH_ROOT_MARK_REASON(ROOT_NAME)
#undef ROOT_NAME
    }
    return "(roots)"_s;
}

// What the collector reports as it marks: each live cell, and each reference
// from one to another or from a root. Parallel markers report at once.
class SnapshotGraph final : public HeapAnalyzer {
public:
    void analyzeNode(JSCell* cell) final
    {
        Locker locker { m_lock };
        if (m_indices.add(cell, m_cells.size()).isNewEntry)
            m_cells.append(cell);
    }

    void analyzeEdge(JSCell* from, JSCell* to, RootMarkReason reason) final
    {
        if (from == to)
            return;
        Locker locker { m_lock };
        m_edges.append({ from, to, from ? EdgeType::Hidden : EdgeType::Element, reason, 0, nullptr });
    }

    void analyzePropertyNameEdge(JSCell* from, JSCell* to, UniquedStringImpl* name) final
    {
        Locker locker { m_lock };
        m_edges.append({ from, to, EdgeType::Property, RootMarkReason::None, 0, name });
    }

    void analyzeVariableNameEdge(JSCell* from, JSCell* to, UniquedStringImpl* name) final
    {
        Locker locker { m_lock };
        m_edges.append({ from, to, EdgeType::Context, RootMarkReason::None, 0, name });
    }

    void analyzeIndexEdge(JSCell* from, JSCell* to, uint32_t index) final
    {
        Locker locker { m_lock };
        m_edges.append({ from, to, EdgeType::Element, RootMarkReason::None, index, nullptr });
    }

    void setOpaqueRootReachabilityReasonForCell(JSCell*, ASCIILiteral) final {}
    void setWrappedObjectForCell(JSCell*, void*) final {}
    void setLabelForCell(JSCell*, const String&) final {}

    // After the collection, on the heap's thread, with collections held off:
    // the cells are all alive.
    Vector<Vector<uint8_t>> write(VM& vm, bool redact, size_t chunkBytes)
    {
        StringTable strings;
        // The synthetic root, "(GC roots)" under it, then a node for each kind
        // of root, then the cells.
        Vector<Node> nodes;
        nodes.append({ NodeType::Synthetic, strings.add(emptyString()), 0, 0 });
        nodes.append({ NodeType::Synthetic, strings.add("(GC roots)"_s), 0, 0 });
        Vector<NodeEdge> edges;
        edges.append({ 0, 1, EdgeType::Element, 1 });

        // Each kind of root's node, 0 while there is none.
        std::array<uint32_t, 256> rootNodes {};
        for (auto& edge : m_edges) {
            if (edge.from)
                continue;
            auto reason = static_cast<uint8_t>(edge.reason);
            if (rootNodes[reason])
                continue;
            uint32_t index = nodes.size();
            rootNodes[reason] = index;
            nodes.append({ NodeType::Synthetic, strings.add(rootName(edge.reason)), 0, 0 });
            edges.append({ 1, index, EdgeType::Element, index - 1 });
        }

        uint32_t firstCell = nodes.size();
        nodes.reserveCapacity(firstCell + m_cells.size());
        for (auto* cell : m_cells) {
            auto [type, name] = describe(vm, cell, redact);
            nodes.append({ type, strings.add(name), cell->estimatedSizeInBytes(vm), 0 });
        }
        m_cells.clear();

        edges.reserveCapacity(edges.size() + m_edges.size());
        for (auto& edge : m_edges) {
            auto to = m_indices.find(edge.to);
            if (to == m_indices.end())
                continue;
            uint32_t from;
            if (!edge.from)
                from = rootNodes[static_cast<uint8_t>(edge.reason)];
            else {
                auto found = m_indices.find(edge.from);
                if (found == m_indices.end())
                    continue;
                from = firstCell + found->value;
            }
            uint32_t nameOrIndex = edge.index;
            if (edge.name)
                nameOrIndex = strings.add(edgeName(edge.name, redact));
            edges.append({ from, firstCell + to->value, edge.type, nameOrIndex });
        }
        m_edges.clear();
        m_indices.clear();

        // A reference can be reported more than once, and both bare and with
        // its name: keep each once, by its name if it has one.
        std::sort(edges.begin(), edges.end(), [](const NodeEdge& a, const NodeEdge& b) {
            return std::tie(a.from, a.to, a.type, a.nameOrIndex) < std::tie(b.from, b.to, b.type, b.nameOrIndex);
        });
        size_t kept = 0;
        for (size_t start = 0; start < edges.size();) {
            size_t end = start;
            bool named = false;
            while (end < edges.size() && edges[end].from == edges[start].from && edges[end].to == edges[start].to) {
                named |= edges[end].type != EdgeType::Hidden;
                end++;
            }
            for (size_t i = start; i < end; i++) {
                auto& edge = edges[i];
                if (named && edge.type == EdgeType::Hidden)
                    continue;
                if (kept && edges[kept - 1].from == edge.from && edges[kept - 1].to == edge.to && edges[kept - 1].type == edge.type && edges[kept - 1].nameOrIndex == edge.nameOrIndex)
                    continue;
                edges[kept++] = edge;
            }
            start = end;
        }
        edges.shrink(kept);
        // A bare reference, and a root, is numbered among its node's.
        uint32_t ordinal = 0;
        for (size_t i = 0; i < edges.size(); i++) {
            auto& edge = edges[i];
            ordinal = i && edges[i - 1].from == edge.from ? ordinal + 1 : 1;
            nodes[edge.from].edgeCount++;
            if (edge.type == EdgeType::Hidden || (edge.from > 0 && edge.from < firstCell && edge.from != 1))
                edge.nameOrIndex = ordinal;
        }

        ChunkWriter out(chunkBytes);
        out.append(snapshotMeta);
        out.append(static_cast<uint64_t>(nodes.size()));
        out.append(",\"edge_count\":");
        out.append(static_cast<uint64_t>(edges.size()));
        out.append(",\"trace_function_count\":0},\n\"nodes\":[");
        for (size_t i = 0; i < nodes.size(); i++) {
            auto& node = nodes[i];
            out.append(i ? ",\n" : "");
            out.append(static_cast<uint64_t>(node.type));
            out.append(",");
            out.append(static_cast<uint64_t>(node.name));
            out.append(",");
            out.append(static_cast<uint64_t>(i * 2 + 1));
            out.append(",");
            out.append(node.selfSize);
            out.append(",");
            out.append(static_cast<uint64_t>(node.edgeCount));
            out.append(",0,0");
        }
        nodes.clear();
        out.append("],\n\"edges\":[");
        for (size_t i = 0; i < edges.size(); i++) {
            auto& edge = edges[i];
            out.append(i ? ",\n" : "");
            out.append(static_cast<uint64_t>(edge.type));
            out.append(",");
            out.append(static_cast<uint64_t>(edge.nameOrIndex));
            out.append(",");
            out.append(static_cast<uint64_t>(edge.to) * nodeFieldCount);
        }
        edges.clear();
        out.append("],\n\"trace_function_infos\":[],\n\"trace_tree\":[],\n\"samples\":[],\n\"locations\":[],\n\"strings\":[");
        auto& table = strings.strings();
        for (size_t i = 0; i < table.size(); i++) {
            out.append(i ? ",\n" : "");
            out.appendQuoted(table[i]);
        }
        out.append("]}\n");
        return out.finish();
    }

private:
    static String edgeName(UniquedStringImpl* name, bool redact)
    {
        if (!name->isSymbol())
            return truncatedName(String(name));
        if (redact)
            return "<symbol>"_s;
        return makeString("<symbol "_s, truncatedName(String(name)), ">"_s);
    }

    Lock m_lock;
    HashMap<JSCell*, uint32_t> m_indices WTF_GUARDED_BY_LOCK(m_lock);
    Vector<JSCell*> m_cells WTF_GUARDED_BY_LOCK(m_lock);
    Vector<CellEdge> m_edges WTF_GUARDED_BY_LOCK(m_lock);
};

#if OS(LINUX)
// The number a cgroup file holds; none for "max" or a file that isn't there.
static std::optional<uint64_t> readNumber(const char* path)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return std::nullopt;
    char buffer[32];
    ssize_t length = read(fd, buffer, sizeof buffer);
    close(fd);
    uint64_t value;
    if (length <= 0 || std::from_chars(buffer, buffer + length, value).ec != std::errc())
        return std::nullopt;
    return value;
}

// What is left under a cgroup v2 limit, of the process's own group as
// /proc/self/cgroup names it, or of the group at the root of a container.
static std::optional<uint64_t> cgroupRoom()
{
    auto room = [](const char* group) -> std::optional<uint64_t> {
        char path[4200];
        snprintf(path, sizeof path, "/sys/fs/cgroup%s/memory.max", group);
        auto limit = readNumber(path);
        snprintf(path, sizeof path, "/sys/fs/cgroup%s/memory.current", group);
        auto used = readNumber(path);
        if (!limit || !used)
            return std::nullopt;
        return *limit > *used ? *limit - *used : 0;
    };
    int fd = open("/proc/self/cgroup", O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        char buffer[4096];
        ssize_t length = read(fd, buffer, sizeof buffer - 1);
        close(fd);
        if (length > 0) {
            buffer[length] = '\0';
            for (char* line = buffer; line && *line;) {
                char* next = strchr(line, '\n');
                if (next)
                    *next++ = '\0';
                if (!strncmp(line, "0::", 3)) {
                    if (auto value = room(line + 3))
                        return value;
                    break;
                }
                line = next;
            }
        }
    }
    return room("");
}
#endif

} // namespace

uint64_t nsolidAvailableMemory()
{
    uint64_t available = Bun__Os__getFreeMemory();
#if OS(LINUX)
    if (auto room = cgroupRoom())
        available = std::min(available, *room);
#endif
    return available;
}

uint64_t nsolidHeapSnapshotCost(VM& vm, bool redact)
{
    return vm.heap.objectCount() * costPerCell + (redact ? 0 : vm.heap.size());
}

Vector<Vector<uint8_t>> takeNSolidHeapSnapshot(VM& vm, bool redact, size_t chunkBytes)
{
    JSLockHolder lock(vm);
    // Collections start only from here until the snapshot is written.
    PreventCollectionScope preventCollection(vm.heap);
    SnapshotGraph graph;
    auto& profiler = vm.ensureHeapProfiler();
    profiler.setActiveHeapAnalyzer(&graph);
    vm.setActiveHeapAnalyzer(&graph);
    vm.heap.collectNow(Sync, CollectionScope::Full);
    vm.setActiveHeapAnalyzer(nullptr);
    profiler.setActiveHeapAnalyzer(nullptr);
    DeferGC deferGC(vm);
    return graph.write(vm, redact, chunkBytes);
}

} // namespace Bun
