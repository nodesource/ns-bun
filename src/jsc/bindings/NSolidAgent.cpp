#include "root.h"
#include "NSolidAgent.h"
#include "ZigGlobalObject.h"
#include "InternalModuleRegistry.h"
#include "ScriptExecutionContext.h"
#include "BunClientData.h"
#include "BunCPUProfiler.h"
#include "BunModuleRegistry.h"
#include "NSolidHeapSnapshot.h"
#include "headers-handwritten.h"
#include "helpers.h"
#include "libusockets.h"
#include <JavaScriptCore/CollectionScope.h>
#include <JavaScriptCore/HeapObserver.h>
#include <JavaScriptCore/JSMapIterator.h>
#include <JavaScriptCore/MutatorState.h>
#include <JavaScriptCore/ModuleRegistryEntry.h>
#include <JavaScriptCore/ObjectConstructor.h>
#include <JavaScriptCore/StackFrame.h>
#include <JavaScriptCore/VMEntryScope.h>
#include <JavaScriptCore/Strong.h>
#include <wtf/Condition.h>
#include <wtf/CryptographicallyRandomNumber.h>
#include <wtf/Function.h>
#include <wtf/Lock.h>
#include <wtf/NeverDestroyed.h>
#include <wtf/ThreadSafeRefCounted.h>
#include <wtf/WallTime.h>
#include <wtf/text/MakeString.h>
#include <algorithm>
#include <cmath>
#include <mutex>

using namespace JSC;
using WebCore::ScriptExecutionContext;

extern "C" void Bun__NSolid__noteRequestedCollection(bool full);

namespace Bun {

// A JS thread's event-loop and GC figures. The thread writes them; the agent's
// thread reads them for the metrics it exports. The loop's own timing is the
// usockets loop's (us_loop_timing); the rest follows N|Solid's EnvInst.
struct NSolidThreadStats : ThreadSafeRefCounted<NSolidThreadStats> {
    static constexpr size_t gcDurationCount = 1000;
    static constexpr size_t gcWindowCount = 256;
    static constexpr size_t idleWindowCount = 1 << 16;

    NSolidThreadStats(us_loop_timing* timing, uint64_t now)
        : timing(timing)
        , registeredNs(now)
        , registeredIdleNs(__atomic_load_n(&timing->idle_ns, __ATOMIC_RELAXED))
        , previousTime(now)
        , previousIdle(registeredIdleNs)
        , previousEvents(__atomic_load_n(&timing->events, __ATOMIC_RELAXED))
        , previousWaiting(__atomic_load_n(&timing->events_waiting, __ATOMIC_RELAXED))
    {
    }

    us_loop_timing* const timing;
    const uint64_t registeredNs;
    const uint64_t registeredIdleNs;

    // N|Solid's per-iteration figures (EnvInst::uv_metrics_cb_).
    std::atomic<uint64_t> loopWithEvents { 0 };
    std::atomic<uint64_t> providerDelay { 0 };
    std::atomic<uint64_t> processingDelay { 0 };
    std::atomic<double> responsiveness[4] {};
    std::atomic<double> rollingEstimatedLagMs { 0 };
    // The previous iteration; only the thread reads and writes these.
    uint64_t previousTime;
    uint64_t previousIdle;
    uint64_t previousEvents;
    uint64_t previousWaiting;
    uint64_t previousProcessing { 0 };

    // Collections, counted the way N|Solid counts V8's: every collection, the
    // full ones (JSC's full collections stand for V8's major ones), the
    // synchronous full collections code asked for, and every collection code
    // asked for.
    std::atomic<uint64_t> gcCount { 0 };
    std::atomic<uint64_t> gcMajorCount { 0 };
    std::atomic<uint64_t> gcFullCount { 0 };
    std::atomic<uint64_t> gcForcedCount { 0 };
    std::atomic<uint64_t> gcStartNs { 0 };
    std::atomic<double> gcStartWallUs { 0 };
    Lock gcLock;
    // The last gcDurationCount collections' wall time in µs, and when the last
    // gcWindowCount ran, in µs since the epoch, as CPU profiles time samples.
    Vector<double> gcDurations WTF_GUARDED_BY_LOCK(gcLock);
    size_t gcNextDuration WTF_GUARDED_BY_LOCK(gcLock) { 0 };
    Vector<std::pair<double, double>> gcWindows WTF_GUARDED_BY_LOCK(gcLock);
    size_t gcNextWindow WTF_GUARDED_BY_LOCK(gcLock) { 0 };

    // Work for the thread that must run even while it is busy in JS
    // (runOnThread).
    Lock tasksLock;
    Vector<Function<void(JSC::VM&)>> tasks WTF_GUARDED_BY_LOCK(tasksLock);
    // The thread's CPU profiler runs for the agent.
    std::atomic<bool> agentProfiling { false };
    // While it does: when the loop waited, in µs since the epoch, and the
    // loop's idle time already in idleWindows. Only the thread uses these.
    Vector<std::pair<double, double>> idleWindows;
    uint64_t recordedIdle { 0 };

    // A blocked loop, as N|Solid finds one: the agent's watchdog marks when
    // the blocked iteration left the poll; the thread, at its next iteration,
    // clears it and leaves how long that iteration ran (ns), which the
    // watchdog reports.
    std::atomic<uint64_t> blockedSinceNs { 0 };
    std::atomic<uint64_t> unblockedAfterNs { 0 };
    // The watchdog's own: the poll exit and the iteration of the last episode
    // it reported, so that it reports each once, and its end as the same loop.
    uint64_t reportedExitNs { 0 };
    uint64_t reportedLoopId { 0 };
};

// A collection's start and end, from the collecting thread or JSC's collector
// thread.
class NSolidGCObserver final : public JSC::HeapObserver {
    WTF_MAKE_NONCOPYABLE(NSolidGCObserver);
    WTF_DEPRECATED_MAKE_FAST_ALLOCATED(NSolidGCObserver);

public:
    NSolidGCObserver(JSC::Heap& heap, Ref<NSolidThreadStats> stats)
        : m_heap(heap)
        , m_stats(WTF::move(stats))
    {
        m_heap.addObserver(this);
    }

    ~NSolidGCObserver() final { m_heap.removeObserver(this); }

private:
    void willGarbageCollect() final
    {
        m_stats->gcStartNs.store(us_loop_timing_now_ns(), std::memory_order_relaxed);
        m_stats->gcStartWallUs.store(WallTime::now().secondsSinceEpoch().microseconds(), std::memory_order_relaxed);
    }

    void didGarbageCollect(JSC::CollectionScope scope) final
    {
        auto& stats = m_stats.get();
        uint64_t start = stats.gcStartNs.load(std::memory_order_relaxed);
        double durationUs = start ? (us_loop_timing_now_ns() - start) / 1000.0 : 0;
        double endWallUs = WallTime::now().secondsSinceEpoch().microseconds();
        stats.gcCount.fetch_add(1, std::memory_order_relaxed);
        if (scope == JSC::CollectionScope::Full)
            stats.gcMajorCount.fetch_add(1, std::memory_order_relaxed);
        Locker locker { stats.gcLock };
        if (stats.gcDurations.size() < NSolidThreadStats::gcDurationCount)
            stats.gcDurations.append(durationUs);
        else
            stats.gcDurations[stats.gcNextDuration] = durationUs;
        stats.gcNextDuration = (stats.gcNextDuration + 1) % NSolidThreadStats::gcDurationCount;
        std::pair<double, double> window { stats.gcStartWallUs.load(std::memory_order_relaxed), endWallUs };
        if (stats.gcWindows.size() < NSolidThreadStats::gcWindowCount)
            stats.gcWindows.append(window);
        else
            stats.gcWindows[stats.gcNextWindow] = window;
        stats.gcNextWindow = (stats.gcNextWindow + 1) % NSolidThreadStats::gcWindowCount;
    }

    JSC::Heap& m_heap;
    Ref<NSolidThreadStats> m_stats;
};

// What a registered thread keeps for itself.
struct NSolidCurrentThread {
    WTF_DEPRECATED_MAKE_FAST_ALLOCATED(NSolidCurrentThread);

public:
    Ref<NSolidThreadStats> stats;
    std::unique_ptr<NSolidGCObserver> gcObserver;
    JSC::VM* vm;
    uint32_t contextId;
};
static thread_local NSolidCurrentThread* currentThread = nullptr;

// While the agent profiles the thread: the loop's waits since the last call,
// as one window that ends when the last of them did. Waits shorter than the
// profiler's interval leave no gap between samples and are left out.
static void recordWaits(NSolidThreadStats& stats, uint64_t now)
{
    uint64_t idle = __atomic_load_n(&stats.timing->idle_ns, __ATOMIC_RELAXED);
    uint64_t waited = idle - std::exchange(stats.recordedIdle, idle);
    uint64_t exit = __atomic_load_n(&stats.timing->exit_ns, __ATOMIC_ACQUIRE);
    if (waited < 1'000'000 || !exit || stats.idleWindows.size() >= NSolidThreadStats::idleWindowCount)
        return;
    double endUs = WallTime::now().secondsSinceEpoch().microseconds() - (now - exit) / 1000.0;
    stats.idleWindows.append({ endUs - waited / 1000.0, endUs });
}

// N|Solid's EnvInst::uv_metrics_cb_, once per loop iteration before the poll,
// on the loop's thread.
static void loopIteration(us_loop_timing* timing, uint64_t now)
{
    auto* current = currentThread;
    if (!current || current->stats->timing != timing)
        return;
    auto& stats = current->stats.get();
    uint64_t idle = __atomic_load_n(&timing->idle_ns, __ATOMIC_RELAXED);
    uint64_t events = __atomic_load_n(&timing->events, __ATOMIC_RELAXED);
    uint64_t waiting = __atomic_load_n(&timing->events_waiting, __ATOMIC_RELAXED);
    if (waiting > stats.previousWaiting)
        stats.loopWithEvents.fetch_add(1, std::memory_order_relaxed);

    uint64_t duration = now - stats.previousTime;
    uint64_t loopIdle = std::min(idle - stats.previousIdle, duration);
    uint64_t processing = duration - loopIdle;
    uint64_t processed = events - stats.previousEvents;
    uint64_t waited = waiting - stats.previousWaiting;
    // How long the waiting events sat in the queue, and how long the processed
    // ones waited to be processed, if each arrived evenly through the iteration.
    uint64_t providerDelay = stats.previousProcessing * waited / 2;
    uint64_t processingDelay = processed == 0 ? 0 : processing * (processed - 1) / 2;
    double responsiveness = processing == 0 || processed == 0 ? 0 : static_cast<double>(providerDelay + processingDelay) / processing;
    double seconds = duration / 1e9;
    static constexpr double timeConstants[] = { 5, 60, 300, 900 };
    for (size_t i = 0; i < std::size(timeConstants); i++) {
        double average = stats.responsiveness[i].load(std::memory_order_relaxed);
        stats.responsiveness[i].store(average + (1 - std::exp(-seconds / timeConstants[i])) * (responsiveness - average), std::memory_order_relaxed);
    }
    double lag = stats.rollingEstimatedLagMs.load(std::memory_order_relaxed);
    double lagSample = waited == 0 ? 0 : stats.previousProcessing / 1e6;
    stats.rollingEstimatedLagMs.store(lag + (1 - std::exp(-seconds)) * (lagSample - lag), std::memory_order_relaxed);

    stats.providerDelay.fetch_add(providerDelay, std::memory_order_relaxed);
    stats.processingDelay.fetch_add(processingDelay, std::memory_order_relaxed);
    stats.previousProcessing = processing;
    stats.previousTime = now;
    stats.previousIdle = idle;
    stats.previousEvents = events;
    stats.previousWaiting = waiting;
    if (stats.agentProfiling.load(std::memory_order_relaxed))
        recordWaits(stats, now);
    if (uint64_t since = stats.blockedSinceNs.load(std::memory_order_relaxed)) {
        if (stats.blockedSinceNs.compare_exchange_strong(since, 0, std::memory_order_acq_rel))
            stats.unblockedAfterNs.store(std::max<uint64_t>(now - since, 1), std::memory_order_release);
    }
}

// The value at `fraction` of the way through `values`, which it reorders.
static double percentile(Vector<double>& values, double fraction)
{
    if (values.isEmpty())
        return 0;
    size_t index = std::min(static_cast<size_t>(values.size() * fraction), values.size() - 1);
    std::nth_element(values.begin(), values.begin() + index, values.end());
    return values[index];
}

// N|Solid's median, which averages the two middle values of an even count.
static double median(Vector<double>& values)
{
    if (values.isEmpty())
        return 0;
    size_t middle = values.size() / 2;
    std::nth_element(values.begin(), values.begin() + middle, values.end());
    double upper = values[middle];
    if (values.size() % 2)
        return upper;
    double lower = *std::max_element(values.begin(), values.begin() + middle);
    return (lower + upper) / 2;
}

// The threads the agent reports on. A thread's id is its context's identifier
// less one, as `worker.threadId` is: the main thread is 0. As in N|Solid, a
// thread's name is empty until the thread names itself.
struct NSolidThread {
    uint32_t contextId;
    WTF::String name;
    RefPtr<NSolidThreadStats> stats;
    JSC::VM* vm { nullptr };
};
static Lock nsolidThreadsLock;
static Vector<NSolidThread>& nsolidThreads() WTF_REQUIRES_LOCK(nsolidThreadsLock)
{
    static NeverDestroyed<Vector<NSolidThread>> threads;
    return threads;
}

// Runs the work queued for the calling thread by runOnThread.
static void runQueuedTasks(JSC::VM& vm)
{
    auto* current = currentThread;
    if (!current)
        return;
    Vector<Function<void(JSC::VM&)>> tasks;
    {
        Locker locker { current->stats->tasksLock };
        tasks = std::exchange(current->stats->tasks, {});
    }
    for (auto& task : tasks)
        task(vm);
}

// Runs `task` once on the thread of `contextId`, even while that thread is
// busy in JS: at its VM's next trap check (JSC's shell-timeout trap, which
// nothing else in Bun uses), or from its event loop if it is waiting there,
// whichever comes first. A thread in a long native call answers once it is
// back in JS. False if the thread is gone.
static bool runOnThread(uint32_t contextId, Function<void(JSC::VM&)>&& task)
{
    {
        Locker locker { nsolidThreadsLock };
        auto& threads = nsolidThreads();
        size_t index = threads.findIf([&](auto& thread) { return thread.contextId == contextId; });
        if (index == notFound || !threads[index].stats || !threads[index].vm)
            return false;
        {
            Locker tasksLocker { threads[index].stats->tasksLock };
            threads[index].stats->tasks.append(WTF::move(task));
        }
        threads[index].vm->notifyNeedShellTimeoutCheck();
    }
    ScriptExecutionContext::postTaskTo(contextId, BunLoopKind::Regular, [](ScriptExecutionContext& context) {
        runQueuedTasks(context.vm());
    });
    return true;
}

// What the agent's requests to other threads are answered with. Only the
// agent's thread reads or writes it.
static JSC::Strong<JSC::Unknown>& replyListener()
{
    static NeverDestroyed<JSC::Strong<JSC::Unknown>> listener;
    return listener;
}

// The process's exit, reported by the agent before the process ends: the
// exiting thread hands the agent what to report and waits until the agent
// says it was sent, or for EXIT_EVENT_TIMEOUT, as N|Solid's does.
static constexpr Seconds EXIT_EVENT_TIMEOUT = 5_s;
static Lock exitLock;
static Condition exitCondition;
static bool exitEventSent WTF_GUARDED_BY_LOCK(exitLock) = false;
// 0 until the agent listens for the exit.
static std::atomic<uint32_t> exitAgentContextId { 0 };
static std::atomic<BunLoopKind> exitAgentLoopKind { BunLoopKind::Regular };

static JSC::Strong<JSC::Unknown>& exitListener()
{
    static NeverDestroyed<JSC::Strong<JSC::Unknown>> listener;
    return listener;
}

// The agent's configuration, as JSON, for `nsolid.config` on any thread: the
// agent publishes it whenever it changes.
static Lock sharedConfigLock;
static WTF::String sharedConfigJSON WTF_GUARDED_BY_LOCK(sharedConfigLock);

// `nsolid.start(config)` on the main thread hands the agent the config it was
// given; calls made before the agent listens wait here for it.
static Lock startLock;
static Vector<WTF::String>& pendingStarts() WTF_REQUIRES_LOCK(startLock)
{
    static NeverDestroyed<Vector<WTF::String>> starts;
    return starts;
}
static uint32_t startAgentContextId WTF_GUARDED_BY_LOCK(startLock) = 0;
static BunLoopKind startAgentLoopKind WTF_GUARDED_BY_LOCK(startLock) = BunLoopKind::Regular;
static JSC::Strong<JSC::Unknown>& startListener()
{
    static NeverDestroyed<JSC::Strong<JSC::Unknown>> listener;
    return listener;
}

// The uncaught exception the process ends with, like N|Solid's saved fatal
// error.
static Lock fatalErrorLock;
static WTF::String fatalErrorMessage WTF_GUARDED_BY_LOCK(fatalErrorLock);
static WTF::String fatalErrorStack WTF_GUARDED_BY_LOCK(fatalErrorLock);

// 20 random bytes as 40 lowercase hex digits, the shape of N|Solid's agent
// ids. One per process, shared by every thread.
static const char* agentId()
{
    static char id[41];
    static std::once_flag once;
    std::call_once(once, [] {
        uint8_t raw[20];
        WTF::cryptographicallyRandomValues(std::span { raw });
        static constexpr char hex[] = "0123456789abcdef";
        for (size_t i = 0; i < std::size(raw); i++) {
            id[i * 2] = hex[raw[i] >> 4];
            id[i * 2 + 1] = hex[raw[i] & 0xf];
        }
        id[40] = '\0';
    });
    return id;
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidAgentId, (JSC::JSGlobalObject * globalObject, JSC::CallFrame*))
{
    return JSValue::encode(jsString(globalObject->vm(), WTF::String::fromLatin1(agentId())));
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidThreads, (JSC::JSGlobalObject * globalObject, JSC::CallFrame*))
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    Vector<NSolidThread> copy;
    {
        Locker locker { nsolidThreadsLock };
        for (auto& thread : nsolidThreads())
            copy.append({ thread.contextId, thread.name.isolatedCopy() });
    }
    auto* array = constructEmptyArray(globalObject, nullptr, copy.size());
    RETURN_IF_EXCEPTION(scope, {});
    for (unsigned i = 0; i < copy.size(); i++) {
        auto* entry = constructEmptyObject(globalObject);
        entry->putDirect(vm, Identifier::fromString(vm, "contextId"_s), jsNumber(copy[i].contextId));
        entry->putDirect(vm, Identifier::fromString(vm, "threadId"_s), jsNumber(copy[i].contextId - 1));
        entry->putDirect(vm, Identifier::fromString(vm, "name"_s), jsString(vm, copy[i].name));
        array->putDirectIndex(globalObject, i, entry);
        RETURN_IF_EXCEPTION(scope, {});
    }
    return JSValue::encode(array);
}

// Each registered thread's loop and GC figures, read on the agent's thread.
// A thread's loop timing lives in that thread's storage, so it is read under
// the registry's lock, which the thread takes before it exits.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidThreadStats, (JSC::JSGlobalObject * globalObject, JSC::CallFrame*))
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    struct Snapshot {
        uint32_t contextId;
        double values[25];
    };
    static constexpr ASCIILiteral names[] = {
        "nowNs"_s, "registeredNs"_s, "registeredIdleNs"_s, "loopStartNs"_s, "loopEntryNs"_s, "loopExitNs"_s,
        "loopIdleTime"_s, "loopIterations"_s, "loopIterWithEvents"_s, "eventsProcessed"_s, "eventsWaiting"_s,
        "providerDelay"_s, "processingDelay"_s, "res5s"_s, "res1m"_s, "res5m"_s, "res15m"_s,
        "rollingEstimatedLag"_s, "gcCount"_s, "gcForcedCount"_s, "gcFullCount"_s, "gcMajorCount"_s,
        "gcDurUsMedian"_s, "gcDurUs99Ptile"_s, "gcCollections"_s
    };
    static_assert(std::size(names) == std::extent_v<decltype(Snapshot::values)>);
    Vector<Snapshot> snapshots;
    {
        Locker locker { nsolidThreadsLock };
        uint64_t now = us_loop_timing_now_ns();
        for (auto& thread : nsolidThreads()) {
            if (!thread.stats)
                continue;
            auto& stats = *thread.stats;
            auto* timing = stats.timing;
            uint64_t entry = __atomic_load_n(&timing->entry_ns, __ATOMIC_ACQUIRE);
            uint64_t idle = __atomic_load_n(&timing->idle_ns, __ATOMIC_RELAXED);
            Vector<double> durations;
            {
                Locker gcLocker { stats.gcLock };
                durations = stats.gcDurations;
            }
            uint64_t collections = durations.size();
            double gcMedian = median(durations);
            double gc99 = percentile(durations, 0.99);
            snapshots.append({ thread.contextId, {
                                                     static_cast<double>(now),
                                                     static_cast<double>(stats.registeredNs),
                                                     static_cast<double>(stats.registeredIdleNs),
                                                     static_cast<double>(__atomic_load_n(&timing->start_ns, __ATOMIC_RELAXED)),
                                                     static_cast<double>(entry),
                                                     static_cast<double>(__atomic_load_n(&timing->exit_ns, __ATOMIC_ACQUIRE)),
                                                     static_cast<double>(idle + (entry && now > entry ? now - entry : 0)),
                                                     static_cast<double>(__atomic_load_n(&timing->iterations, __ATOMIC_RELAXED)),
                                                     static_cast<double>(stats.loopWithEvents.load(std::memory_order_relaxed)),
                                                     static_cast<double>(__atomic_load_n(&timing->events, __ATOMIC_RELAXED)),
                                                     static_cast<double>(__atomic_load_n(&timing->events_waiting, __ATOMIC_RELAXED)),
                                                     static_cast<double>(stats.providerDelay.load(std::memory_order_relaxed)),
                                                     static_cast<double>(stats.processingDelay.load(std::memory_order_relaxed)),
                                                     stats.responsiveness[0].load(std::memory_order_relaxed),
                                                     stats.responsiveness[1].load(std::memory_order_relaxed),
                                                     stats.responsiveness[2].load(std::memory_order_relaxed),
                                                     stats.responsiveness[3].load(std::memory_order_relaxed),
                                                     stats.rollingEstimatedLagMs.load(std::memory_order_relaxed),
                                                     static_cast<double>(stats.gcCount.load(std::memory_order_relaxed)),
                                                     static_cast<double>(stats.gcForcedCount.load(std::memory_order_relaxed)),
                                                     static_cast<double>(stats.gcFullCount.load(std::memory_order_relaxed)),
                                                     static_cast<double>(stats.gcMajorCount.load(std::memory_order_relaxed)),
                                                     gcMedian,
                                                     gc99,
                                                     static_cast<double>(collections),
                                                 } });
        }
    }
    auto* array = constructEmptyArray(globalObject, nullptr, snapshots.size());
    RETURN_IF_EXCEPTION(scope, {});
    for (unsigned i = 0; i < snapshots.size(); i++) {
        auto* entry = constructEmptyObject(globalObject);
        entry->putDirect(vm, Identifier::fromString(vm, "threadId"_s), jsNumber(snapshots[i].contextId - 1));
        for (size_t field = 0; field < std::size(names); field++)
            entry->putDirect(vm, Identifier::fromString(vm, names[field]), jsNumber(snapshots[i].values[field]));
        array->putDirectIndex(globalObject, i, entry);
        RETURN_IF_EXCEPTION(scope, {});
    }
    return JSValue::encode(array);
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidOnReply, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    replyListener().set(globalObject->vm(), callFrame->argument(0));
    return JSValue::encode(jsUndefined());
}

// Calls the agent's reply listener with `(requestId, make())`, on the agent's
// thread. `make` holds only what may cross threads.
template<typename Make>
static void replyToAgent(ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId, Make&& make)
{
    ScriptExecutionContext::postTaskTo(agentContextId, agentLoopKind, [requestId, make = std::forward<Make>(make)](ScriptExecutionContext& agent) mutable {
        auto* agentGlobal = agent.jsGlobalObject();
        auto& agentVM = agent.vm();
        JSValue listener = replyListener().get();
        if (!agentGlobal || !listener || !listener.isCallable())
            return;
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(agentVM);
        JSValue value = make(agentVM, agentGlobal);
        if (!scope.exception()) {
            MarkedArgumentBuffer arguments;
            arguments.append(jsNumber(requestId));
            arguments.append(value);
            JSC::call(agentGlobal, listener, arguments, "N|Solid reply"_s);
        }
        if (auto* exception = scope.exception()) {
            scope.clearException();
            Zig::GlobalObject::reportUncaughtExceptionAtEventLoop(agentGlobal, exception);
        }
    });
}

// Runs `task(thread)` on the thread of `contextId` for a request of the agent,
// which `task` answers through `replyToAgent`. False if that thread is gone.
template<typename Task>
static EncodedJSValue requestOfThread(JSC::JSGlobalObject* globalObject, JSC::CallFrame* callFrame, Task&& task)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    uint32_t contextId = callFrame->argument(0).toUInt32(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    double requestId = callFrame->argument(1).toNumber(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    auto* agentContext = defaultGlobalObject(globalObject)->scriptExecutionContext();
    auto agentContextId = agentContext->identifier();
    auto agentLoopKind = agentContext->currentLoopKind();
    bool posted = ScriptExecutionContext::postTaskTo(contextId, BunLoopKind::Regular, [agentContextId, agentLoopKind, requestId, task = std::forward<Task>(task)](ScriptExecutionContext& thread) mutable {
        task(thread, agentContextId, agentLoopKind, requestId);
    });
    return JSValue::encode(jsBoolean(posted));
}

// Like requestOfThread, but `task(vm, ...)` runs even while the thread is busy
// in JS (runOnThread).
template<typename Task>
static EncodedJSValue requestOfBusyThread(JSC::JSGlobalObject* globalObject, JSC::CallFrame* callFrame, Task&& task)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    uint32_t contextId = callFrame->argument(0).toUInt32(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    double requestId = callFrame->argument(1).toNumber(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    auto* agentContext = defaultGlobalObject(globalObject)->scriptExecutionContext();
    auto agentContextId = agentContext->identifier();
    auto agentLoopKind = agentContext->currentLoopKind();
    bool queued = runOnThread(contextId, [agentContextId, agentLoopKind, requestId, task = std::forward<Task>(task)](JSC::VM& threadVM) mutable {
        task(threadVM, agentContextId, agentLoopKind, requestId);
    });
    return JSValue::encode(jsBoolean(queued));
}

// The calling thread's recent collections, as [start, end, start, end, ...]
// in µs since the epoch.
static Vector<double> recentCollections(NSolidThreadStats& stats)
{
    Vector<double> windows;
    Locker locker { stats.gcLock };
    for (auto& [start, end] : stats.gcWindows) {
        windows.append(start);
        windows.append(end);
    }
    return windows;
}

// For the agent: starts a CPU profile on a thread. Answers { startedAt } (µs
// since the epoch), or { inUse: true } when the thread's profiler is taken.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidStartProfile, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    return requestOfBusyThread(globalObject, callFrame, [](JSC::VM& threadVM, ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId) {
        auto* current = currentThread;
        bool inUse = !current || Bun::isCPUProfilerRunning() || current->stats->agentProfiling.load();
        double startedAt = 0;
        if (!inUse) {
            Bun::startCPUProfiler(threadVM);
            current->stats->idleWindows.clear();
            current->stats->recordedIdle = __atomic_load_n(&current->stats->timing->idle_ns, __ATOMIC_RELAXED);
            current->stats->agentProfiling.store(true);
            startedAt = WallTime::now().secondsSinceEpoch().microseconds();
        }
        replyToAgent(agentContextId, agentLoopKind, requestId, [inUse, startedAt](VM& vm, JSC::JSGlobalObject* global) -> JSValue {
            auto* answer = constructEmptyObject(global);
            if (inUse)
                answer->putDirect(vm, Identifier::fromString(vm, "inUse"_s), jsBoolean(true));
            else
                answer->putDirect(vm, Identifier::fromString(vm, "startedAt"_s), jsNumber(startedAt));
            return answer;
        });
    });
}

// A CPU profile the agent took, with what the agent needs to complete it: when
// it stopped, and when the thread collected and when its loop waited, as
// [start, end, start, end, ...]. Times are µs since the epoch. Copies that may
// cross threads; `json` is null when there is no profile.
struct AgentProfile {
    WTF::String json;
    double stoppedAt { 0 };
    Vector<double> collections;
    Vector<double> waits;
};

// Stops the agent's CPU profile on the calling thread.
static AgentProfile stopAgentProfile(JSC::VM& vm)
{
    auto* current = currentThread;
    if (!current || !current->stats->agentProfiling.exchange(false) || !Bun::isCPUProfilerRunning())
        return {};
    auto& stats = current->stats.get();
    WTF::String json;
    Bun::stopCPUProfiler(vm, &json, nullptr);
    recordWaits(stats, us_loop_timing_now_ns());
    Vector<double> waits;
    waits.reserveInitialCapacity(stats.idleWindows.size() * 2);
    for (auto& [start, end] : std::exchange(stats.idleWindows, {})) {
        waits.append(start);
        waits.append(end);
    }
    return { json.isolatedCopy(), WallTime::now().secondsSinceEpoch().microseconds(), recentCollections(stats), WTF::move(waits) };
}

static JSArray* numbersArray(JSC::JSGlobalObject* global, const Vector<double>& numbers)
{
    auto scope = DECLARE_THROW_SCOPE(global->vm());
    auto* array = constructEmptyArray(global, nullptr, numbers.size());
    RETURN_IF_EXCEPTION(scope, nullptr);
    for (unsigned i = 0; i < numbers.size(); i++) {
        array->putDirectIndex(global, i, jsNumber(numbers[i]));
        RETURN_IF_EXCEPTION(scope, nullptr);
    }
    return array;
}

static JSValue profileAnswer(VM& vm, JSC::JSGlobalObject* global, const AgentProfile& profile)
{
    auto scope = DECLARE_THROW_SCOPE(vm);
    if (profile.json.isNull())
        return jsUndefined();
    auto* answer = constructEmptyObject(global);
    answer->putDirect(vm, Identifier::fromString(vm, "profile"_s), jsString(vm, profile.json));
    answer->putDirect(vm, Identifier::fromString(vm, "stoppedAt"_s), jsNumber(profile.stoppedAt));
    auto* collections = numbersArray(global, profile.collections);
    RETURN_IF_EXCEPTION(scope, {});
    answer->putDirect(vm, Identifier::fromString(vm, "collections"_s), collections);
    auto* waits = numbersArray(global, profile.waits);
    RETURN_IF_EXCEPTION(scope, {});
    answer->putDirect(vm, Identifier::fromString(vm, "waits"_s), waits);
    return answer;
}

// For the agent: stops its CPU profile on a thread. Answers
// { profile, stoppedAt, collections }, or { stopped: false } when the agent's
// profile wasn't running there.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidStopProfile, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    return requestOfBusyThread(globalObject, callFrame, [](JSC::VM& threadVM, ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId) {
        replyToAgent(agentContextId, agentLoopKind, requestId, [profile = stopAgentProfile(threadVM)](VM& vm, JSC::JSGlobalObject* global) -> JSValue {
            if (profile.json.isNull()) {
                auto* answer = constructEmptyObject(global);
                answer->putDirect(vm, Identifier::fromString(vm, "stopped"_s), jsBoolean(false));
                return answer;
            }
            return profileAnswer(vm, global, profile);
        });
    });
}

// A heap snapshot of the calling thread for the agent, answered with
// { chunks: Uint8Array[] }, or { error: "memory" } when it would take more than
// half the memory the process has left (N|Solid's out-of-memory error).
static void snapshotForAgent(JSC::VM& vm, bool redact, size_t chunkBytes, ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId)
{
    auto* current = currentThread;
    if (!current)
        return;
    // A collection can't start here (the thread is in one, or in code that
    // holds them off): the snapshot waits for the thread's event loop.
    if (vm.heap.isDeferred() || vm.heap.mutatorState() != JSC::MutatorState::Running) {
        ScriptExecutionContext::postTaskTo(current->contextId, BunLoopKind::Regular, [redact, chunkBytes, agentContextId, agentLoopKind, requestId](ScriptExecutionContext& context) {
            snapshotForAgent(context.vm(), redact, chunkBytes, agentContextId, agentLoopKind, requestId);
        });
        return;
    }
    if (nsolidHeapSnapshotCost(vm, redact) > nsolidAvailableMemory() / 2) {
        replyToAgent(agentContextId, agentLoopKind, requestId, [](VM& vm, JSC::JSGlobalObject* global) -> JSValue {
            auto* answer = constructEmptyObject(global);
            answer->putDirect(vm, Identifier::fromString(vm, "error"_s), jsString(vm, String("memory"_s)));
            return answer;
        });
        return;
    }
    Bun__NSolid__noteRequestedCollection(true);
    auto chunks = takeNSolidHeapSnapshot(vm, redact, chunkBytes);
    replyToAgent(agentContextId, agentLoopKind, requestId, [chunks = WTF::move(chunks)](VM& vm, JSC::JSGlobalObject* global) mutable -> JSValue {
        auto scope = DECLARE_THROW_SCOPE(vm);
        auto* array = constructEmptyArray(global, nullptr, chunks.size());
        RETURN_IF_EXCEPTION(scope, {});
        for (unsigned i = 0; i < chunks.size(); i++) {
            size_t length = chunks[i].size();
            auto bytes = chunks[i].releaseBuffer().leakSpan();
            auto buffer = ArrayBuffer::createFromBytes(std::span<const uint8_t> { bytes.data(), length }, createSharedTask<void(void*)>([](void* p) {
                fastFree(p);
            }));
            auto* chunk = JSC::JSUint8Array::create(global, global->typedArrayStructureWithTypedArrayType<TypeUint8>(), WTF::move(buffer), 0, length);
            RETURN_IF_EXCEPTION(scope, {});
            array->putDirectIndex(global, i, chunk);
            RETURN_IF_EXCEPTION(scope, {});
        }
        auto* answer = constructEmptyObject(global);
        answer->putDirect(vm, Identifier::fromString(vm, "chunks"_s), array);
        return answer;
    });
}

// For the agent: a heap snapshot of a thread (see snapshotForAgent), with
// (contextId, requestId, redact, chunkBytes).
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidTakeSnapshot, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    bool redact = callFrame->argument(2).toBoolean(globalObject);
    size_t chunkBytes = callFrame->argument(3).toUInt32(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    RELEASE_AND_RETURN(scope, requestOfBusyThread(globalObject, callFrame, [redact, chunkBytes](JSC::VM& threadVM, ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId) {
        snapshotForAgent(threadVM, redact, chunkBytes, agentContextId, agentLoopKind, requestId);
    }));
}

// For the agent's blocked-loop watchdog, every 100 ms: the threads whose loop
// has been out of its poll for at least `thresholdMs` since the last check,
// once per episode, and the ones that have since iterated again, as
// { contextId, threadId, blocked, blockedFor (ms), loopId, callbacks }.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidCheckLoops, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    double thresholdMs = callFrame->argument(0).toNumber(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    uint64_t thresholdNs = thresholdMs > 0 ? static_cast<uint64_t>(thresholdMs * 1e6) : 0;
    struct LoopEvent {
        uint32_t contextId;
        bool blocked;
        uint64_t blockedForMs;
        uint64_t loopId;
        uint64_t callbacks;
    };
    Vector<LoopEvent> events;
    {
        Locker locker { nsolidThreadsLock };
        uint64_t now = us_loop_timing_now_ns();
        for (auto& thread : nsolidThreads()) {
            if (!thread.stats)
                continue;
            auto& stats = *thread.stats;
            auto* timing = stats.timing;
            uint64_t loopId = __atomic_load_n(&timing->iterations, __ATOMIC_RELAXED);
            uint64_t callbacks = __atomic_load_n(&timing->events, __ATOMIC_RELAXED);
            if (uint64_t ran = stats.unblockedAfterNs.exchange(0, std::memory_order_acquire))
                events.append({ thread.contextId, false, (ran + 999'999) / 1'000'000, stats.reportedLoopId, callbacks });
            uint64_t entry = __atomic_load_n(&timing->entry_ns, __ATOMIC_ACQUIRE);
            uint64_t exit = __atomic_load_n(&timing->exit_ns, __ATOMIC_ACQUIRE);
            if (!thresholdNs || entry || !exit || now < exit + thresholdNs || exit == stats.reportedExitNs)
                continue;
            stats.reportedExitNs = exit;
            stats.reportedLoopId = loopId;
            stats.blockedSinceNs.store(exit, std::memory_order_release);
            events.append({ thread.contextId, true, (now - exit) / 1'000'000, loopId, callbacks });
        }
    }
    auto* array = constructEmptyArray(globalObject, nullptr, events.size());
    RETURN_IF_EXCEPTION(scope, {});
    for (unsigned i = 0; i < events.size(); i++) {
        auto& event = events[i];
        auto* object = constructEmptyObject(globalObject);
        object->putDirect(vm, Identifier::fromString(vm, "contextId"_s), jsNumber(event.contextId));
        object->putDirect(vm, Identifier::fromString(vm, "threadId"_s), jsNumber(event.contextId - 1));
        object->putDirect(vm, Identifier::fromString(vm, "blocked"_s), jsBoolean(event.blocked));
        object->putDirect(vm, Identifier::fromString(vm, "blockedFor"_s), jsNumber(event.blockedForMs));
        object->putDirect(vm, Identifier::fromString(vm, "loopId"_s), jsNumber(event.loopId));
        object->putDirect(vm, Identifier::fromString(vm, "callbacks"_s), jsNumber(event.callbacks));
        array->putDirectIndex(globalObject, i, object);
        RETURN_IF_EXCEPTION(scope, {});
    }
    return JSValue::encode(array);
}

// A frame of a blocked thread's stack, as N|Solid reports it.
struct LoopStackFrame {
    bool isEval;
    WTF::String scriptName;
    WTF::String functionName;
    unsigned line;
    unsigned column;
    uint64_t scriptId;
};
static constexpr size_t loopStackFrameCount = 100;

// The calling thread's JavaScript stack, up to 100 frames, as V8 gives N|Solid
// one: script frames only, lines and columns from 1 in the source as written
// (source-mapped, as Error.stack is), no script name for eval code, and no
// function name for a script's top level.
static Vector<LoopStackFrame> loopStackOf(JSC::VM& vm, JSC::JSGlobalObject* global)
{
    void* bunVM = clientData(vm)->bunVM;
    Vector<JSC::StackFrame> frames;
    vm.interpreter.getStackTrace(global, frames, 0, loopStackFrameCount * 2);
    Vector<LoopStackFrame> stack;
    for (auto& frame : frames) {
        if (stack.size() == loopStackFrameCount)
            break;
        if (!frame.hasLineAndColumnInfo() || frame.isBuiltinFunction())
            continue;
        auto codeType = frame.codeType();
        bool isEval = codeType == JSC::EvalCode;
        bool isFunction = codeType == JSC::FunctionCode;
        auto position = frame.computeLineAndColumn();
        ZigStackFrame source;
        source.source_url = Bun::toStringRef(frame.sourceURL(vm));
        source.position.line_zero_based = position.line ? position.line - 1 : 0;
        source.position.column_zero_based = position.column ? position.column - 1 : 0;
        Bun__remapStackFramePositions(bunVM, &source, 1);
        stack.append({
            isEval,
            isEval ? WTF::String() : source.source_url.toWTFString().isolatedCopy(),
            isFunction ? frame.functionName(vm).isolatedCopy() : WTF::emptyString(),
            static_cast<unsigned>(source.position.line_zero_based + 1),
            static_cast<unsigned>(source.position.column_zero_based + 1),
            static_cast<uint64_t>(frame.sourceID()),
        });
    }
    return stack;
}

// For the agent: the JavaScript stack of a thread the watchdog found blocked
// (contextId, requestId), taken while it is still in that iteration (null
// after). Answered like the other requests.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidLoopStack, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    return requestOfBusyThread(globalObject, callFrame, [](JSC::VM& threadVM, ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId) {
        auto* current = currentThread;
        std::optional<Vector<LoopStackFrame>> stack;
        if (current && current->stats->blockedSinceNs.load(std::memory_order_acquire)) {
            if (auto* global = threadVM.entryScope ? threadVM.entryScope->globalObject() : nullptr)
                stack = loopStackOf(threadVM, global);
            else
                stack = Vector<LoopStackFrame> {};
        }
        replyToAgent(agentContextId, agentLoopKind, requestId, [stack = WTF::move(stack)](VM& vm, JSC::JSGlobalObject* global) -> JSValue {
            auto scope = DECLARE_THROW_SCOPE(vm);
            if (!stack)
                return jsNull();
            auto* array = constructEmptyArray(global, nullptr, stack->size());
            RETURN_IF_EXCEPTION(scope, {});
            for (unsigned i = 0; i < stack->size(); i++) {
                auto& frame = (*stack)[i];
                auto* object = constructEmptyObject(global);
                object->putDirect(vm, Identifier::fromString(vm, "is_eval"_s), jsBoolean(frame.isEval));
                if (!frame.scriptName.isNull())
                    object->putDirect(vm, Identifier::fromString(vm, "script_name"_s), jsString(vm, frame.scriptName));
                object->putDirect(vm, Identifier::fromString(vm, "function_name"_s), jsString(vm, frame.functionName));
                object->putDirect(vm, Identifier::fromString(vm, "line_number"_s), jsNumber(frame.line));
                object->putDirect(vm, Identifier::fromString(vm, "column"_s), jsNumber(frame.column));
                object->putDirect(vm, Identifier::fromString(vm, "script_id"_s), jsNumber(frame.scriptId));
                array->putDirectIndex(global, i, object);
                RETURN_IF_EXCEPTION(scope, {});
            }
            return array;
        });
    });
}

// The keys of the ES module registry and of require.cache, as the inspector's
// getModuleGraph reads them: most are file paths. Copies that may cross
// threads.
static Vector<WTF::String> loadedModulesOf(Zig::GlobalObject* global)
{
    Vector<WTF::String> modules;
    if (!global)
        return modules;
    auto& vm = global->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    Bun::forEachModuleRegistrySpecifier(global->moduleLoader(), [&](UniquedStringImpl* specifier, JSC::ModuleRegistryEntry*) {
        modules.append(WTF::String { specifier }.isolatedCopy());
    });
    auto* iterator = JSC::JSMapIterator::create(vm, global->mapIteratorStructure(), global->requireMap(), JSC::IterationKind::Keys);
    if (!scope.exception()) {
        JSValue key;
        while (iterator->next(global, key) && !scope.exception()) {
            if (key.isString())
                modules.append(key.toWTFString(global).isolatedCopy());
        }
    }
    if (scope.exception())
        scope.clearException();
    return modules;
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidRequestThreadHeap, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    return requestOfThread(globalObject, callFrame, [](ScriptExecutionContext& thread, ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId) {
        // The figures process.memoryUsage() reports, read on the thread they
        // belong to. heap.size() would walk the heap, so the used size is the
        // one JSC measured at the end of the last collection.
        auto& threadVM = thread.vm();
        double heapTotal = threadVM.heap.blockBytesAllocated();
        double heapUsed = WebCore::clientData(threadVM)->heapSizeAfterLastCollection();
        if (!heapUsed)
            heapUsed = heapTotal;
        double external = threadVM.heap.extraMemorySize() + threadVM.heap.externalMemorySize();
        double arrayBuffers = threadVM.heap.arrayBufferSize();
        replyToAgent(agentContextId, agentLoopKind, requestId, [heapTotal, heapUsed, external, arrayBuffers](VM& vm, JSC::JSGlobalObject* global) -> JSValue {
            auto* figures = constructEmptyObject(global);
            figures->putDirect(vm, Identifier::fromString(vm, "heapTotal"_s), jsNumber(heapTotal));
            figures->putDirect(vm, Identifier::fromString(vm, "heapUsed"_s), jsNumber(heapUsed));
            figures->putDirect(vm, Identifier::fromString(vm, "external"_s), jsNumber(external));
            figures->putDirect(vm, Identifier::fromString(vm, "arrayBuffers"_s), jsNumber(arrayBuffers));
            return figures;
        });
    });
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidRequestLoadedModules, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    return requestOfThread(globalObject, callFrame, [](ScriptExecutionContext& thread, ScriptExecutionContextIdentifier agentContextId, BunLoopKind agentLoopKind, double requestId) {
        Vector<WTF::String> modules = loadedModulesOf(defaultGlobalObject(thread.jsGlobalObject()));
        replyToAgent(agentContextId, agentLoopKind, requestId, [modules = WTF::move(modules)](VM& vm, JSC::JSGlobalObject* global) -> JSValue {
            MarkedArgumentBuffer values;
            for (auto& module : modules)
                values.append(jsString(vm, module));
            return constructArray(global, static_cast<ArrayAllocationProfile*>(nullptr), values);
        });
    });
}

}

namespace Bun {

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidOnExit, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    exitListener().set(globalObject->vm(), callFrame->argument(0));
    auto* context = defaultGlobalObject(globalObject)->scriptExecutionContext();
    exitAgentLoopKind.store(context->currentLoopKind());
    exitAgentContextId.store(context->identifier());
    return JSValue::encode(jsUndefined());
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidExitSent, (JSC::JSGlobalObject*, JSC::CallFrame*))
{
    Locker locker { exitLock };
    exitEventSent = true;
    exitCondition.notifyAll();
    return JSValue::encode(jsUndefined());
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidPublishConfig, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    WTF::String json = callFrame->argument(0).toWTFString(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    Locker locker { sharedConfigLock };
    sharedConfigJSON = json.isolatedCopy();
    return JSValue::encode(jsUndefined());
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidSharedConfig, (JSC::JSGlobalObject * globalObject, JSC::CallFrame*))
{
    WTF::String json;
    {
        Locker locker { sharedConfigLock };
        json = sharedConfigJSON.isolatedCopy();
    }
    if (json.isNull())
        return JSValue::encode(jsUndefined());
    return JSValue::encode(jsString(globalObject->vm(), json));
}

// The agent listens for `nsolid.start(config)`, and gets the calls made
// before it did.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidOnStart, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    auto& vm = globalObject->vm();
    startListener().set(vm, callFrame->argument(0));
    auto* context = defaultGlobalObject(globalObject)->scriptExecutionContext();
    Vector<WTF::String> pending;
    {
        Locker locker { startLock };
        startAgentContextId = context->identifier();
        startAgentLoopKind = context->currentLoopKind();
        pending.swap(pendingStarts());
    }
    MarkedArgumentBuffer values;
    for (auto& json : pending)
        values.append(jsString(vm, json));
    return JSValue::encode(constructArray(globalObject, static_cast<ArrayAllocationProfile*>(nullptr), values));
}

}

extern "C" bool Bun__NSolid__startFromScript(JSC::JSGlobalObject*);
extern "C" double Bun__NSolid__processStartMs();

namespace Bun {

// `nsolid.start(config)`: the config, as JSON, goes to the agent, which is
// started if it was not. False when there is no agent to start.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidStart, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    WTF::String json = callFrame->argument(0).toWTFString(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    uint32_t agentContextId;
    BunLoopKind agentLoopKind;
    {
        Locker locker { startLock };
        agentContextId = startAgentContextId;
        agentLoopKind = startAgentLoopKind;
        if (!agentContextId)
            pendingStarts().append(json.isolatedCopy());
    }
    if (!agentContextId)
        return JSValue::encode(jsBoolean(Bun__NSolid__startFromScript(globalObject)));
    ScriptExecutionContext::postTaskTo(agentContextId, agentLoopKind, [json = json.isolatedCopy()](ScriptExecutionContext& agent) {
        auto* agentGlobal = agent.jsGlobalObject();
        JSValue listener = startListener().get();
        if (!agentGlobal || !listener || !listener.isCallable())
            return;
        auto& agentVM = agent.vm();
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(agentVM);
        MarkedArgumentBuffer arguments;
        arguments.append(jsString(agentVM, json));
        JSC::call(agentGlobal, listener, arguments, "N|Solid start"_s);
        if (auto* exception = scope.exception()) {
            scope.clearException();
            Zig::GlobalObject::reportUncaughtExceptionAtEventLoop(agentGlobal, exception);
        }
    });
    return JSValue::encode(jsBoolean(true));
}

// What `nsolid.profile()`, `profileEnd()` and `snapshot()` hand the agent:
// called on the agent's thread with (command, threadId, value). Only the
// agent's thread reads or writes it.
static JSC::Strong<JSC::Unknown>& apiListener()
{
    static NeverDestroyed<JSC::Strong<JSC::Unknown>> listener;
    return listener;
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidOnApiCommand, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    apiListener().set(globalObject->vm(), callFrame->argument(0));
    return JSValue::encode(jsUndefined());
}

// Calls the agent's API listener with (command, threadId, make()) on the
// agent's thread. False when no agent listens. `make` holds only what may
// cross threads.
template<typename Make>
static bool postToAgentApi(ASCIILiteral command, uint32_t threadId, Make&& make)
{
    uint32_t agentContextId;
    BunLoopKind agentLoopKind;
    {
        Locker locker { startLock };
        agentContextId = startAgentContextId;
        agentLoopKind = startAgentLoopKind;
    }
    if (!agentContextId)
        return false;
    return ScriptExecutionContext::postTaskTo(agentContextId, agentLoopKind, [command, threadId, make = std::forward<Make>(make)](ScriptExecutionContext& agent) mutable {
        auto* agentGlobal = agent.jsGlobalObject();
        JSValue listener = apiListener().get();
        if (!agentGlobal || !listener || !listener.isCallable())
            return;
        auto& agentVM = agent.vm();
        auto scope = DECLARE_TOP_EXCEPTION_SCOPE(agentVM);
        JSValue value = make(agentVM, agentGlobal);
        if (!scope.exception()) {
            MarkedArgumentBuffer arguments;
            arguments.append(jsString(agentVM, String(command)));
            arguments.append(jsNumber(threadId));
            arguments.append(value);
            JSC::call(agentGlobal, listener, arguments, "N|Solid API"_s);
        }
        if (auto* exception = scope.exception()) {
            scope.clearException();
            Zig::GlobalObject::reportUncaughtExceptionAtEventLoop(agentGlobal, exception);
        }
    });
}

// N|Solid's statuses for the `nsolid` asset calls: 0, or the code of the
// error (its grpc_errors.h).
static constexpr int32_t apiNotAvailable = 404;
static constexpr int32_t apiInProgress = 409;

// `nsolid.profile(timeout)` and `nsolid.snapshot()` on the calling thread:
// the agent takes them as it takes the Console's commands, and sends them to
// the Console. The status N|Solid returns for what is known at once.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidApiCommand, (JSC::JSGlobalObject * globalObject, JSC::CallFrame* callFrame))
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_THROW_SCOPE(vm);
    WTF::String command = callFrame->argument(0).toWTFString(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    double duration = callFrame->argument(1).toNumber(globalObject);
    RETURN_IF_EXCEPTION(scope, {});
    auto* current = currentThread;
    if (!current)
        return JSValue::encode(jsNumber(apiNotAvailable));
    uint32_t threadId = current->contextId - 1;
    bool posted = false;
    if (command == "profile"_s) {
        if (Bun::isCPUProfilerRunning() || current->stats->agentProfiling.load())
            return JSValue::encode(jsNumber(apiInProgress));
        posted = postToAgentApi("profile"_s, threadId, [duration](VM&, JSC::JSGlobalObject*) -> JSValue { return jsNumber(duration); });
    } else if (command == "snapshot"_s)
        posted = postToAgentApi("snapshot"_s, threadId, [](VM&, JSC::JSGlobalObject*) -> JSValue { return jsUndefined(); });
    return JSValue::encode(jsNumber(posted ? 0 : apiNotAvailable));
}

// `nsolid.profileEnd()`: stops the agent's profile of the calling thread,
// which the agent then sends.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidApiProfileEnd, (JSC::JSGlobalObject * globalObject, JSC::CallFrame*))
{
    auto* current = currentThread;
    if (!current)
        return JSValue::encode(jsNumber(apiNotAvailable));
    auto profile = stopAgentProfile(globalObject->vm());
    if (profile.json.isNull())
        return JSValue::encode(jsNumber(apiNotAvailable));
    bool posted = postToAgentApi("profileEnd"_s, current->contextId - 1, [profile = WTF::move(profile)](VM& vm, JSC::JSGlobalObject* global) -> JSValue {
        return profileAnswer(vm, global, profile);
    });
    return JSValue::encode(jsNumber(posted ? 0 : apiNotAvailable));
}

// When the process started, in milliseconds since the epoch: the same on
// every thread.
JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidProcessStart, (JSC::JSGlobalObject*, JSC::CallFrame*))
{
    return JSValue::encode(jsNumber(Bun__NSolid__processStartMs()));
}

JSC_DEFINE_HOST_FUNCTION(jsFunctionNSolidLoadedModules, (JSC::JSGlobalObject * globalObject, JSC::CallFrame*))
{
    auto& vm = globalObject->vm();
    MarkedArgumentBuffer values;
    for (auto& module : loadedModulesOf(defaultGlobalObject(globalObject)))
        values.append(jsString(vm, module));
    return JSValue::encode(constructArray(globalObject, static_cast<ArrayAllocationProfile*>(nullptr), values));
}

}

// An uncaught exception the main thread ends with: what V8 would call its
// message ("Uncaught Error: boom") and the error's stack.
extern "C" void Bun__NSolid__recordFatalError(JSC::JSGlobalObject* globalObject, JSC::EncodedJSValue encodedError)
{
    auto& vm = globalObject->vm();
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);
    JSValue error = JSValue::decode(encodedError);
    // An own or inherited string property, read without running a getter's
    // exception into the exit path.
    auto stringProperty = [&](JSObject* object, const Identifier& name) -> WTF::String {
        JSValue value = object->getIfPropertyExists(globalObject, name);
        WTF::String string;
        if (!scope.exception() && value && value.isString())
            string = asString(value)->value(globalObject);
        if (scope.exception())
            scope.clearException();
        return string;
    };
    WTF::String text;
    WTF::String stack;
    if (auto* object = error.getObject()) {
        // What Error.prototype.toString gives.
        WTF::String name = stringProperty(object, vm.propertyNames->name);
        WTF::String message = stringProperty(object, vm.propertyNames->message);
        if (name.isNull())
            name = "Error"_s;
        text = message.isEmpty() ? name : makeString(name, ": "_s, message);
        stack = stringProperty(object, vm.propertyNames->stack);
    } else if (error.isPrimitive()) {
        text = error.toWTFString(globalObject);
        if (scope.exception()) {
            scope.clearException();
            text = WTF::String();
        }
    }
    Locker locker { Bun::fatalErrorLock };
    Bun::fatalErrorMessage = makeString("Uncaught "_s, text).isolatedCopy();
    Bun::fatalErrorStack = stack.isolatedCopy();
}

// A crash, from the crash handler (src/jsc/nsolid_agent.rs): its reason and
// trace string are what the process's exit reports.
extern "C" void Bun__NSolid__recordCrash(const uint8_t* reason, size_t reasonLength, const uint8_t* trace, size_t traceLength)
{
    Locker locker { Bun::fatalErrorLock };
    Bun::fatalErrorMessage = WTF::String::fromUTF8ReplacingInvalidSequences(std::span { reason, reasonLength }).isolatedCopy();
    Bun::fatalErrorStack = WTF::String::fromUTF8ReplacingInvalidSequences(std::span { trace, traceLength }).isolatedCopy();
}

// On the thread the process ends on. `signal` is the name of the signal that
// ends it, or null.
extern "C" void Bun__NSolid__reportExit(int32_t code, const char* signal)
{
    uint32_t agentContextId = Bun::exitAgentContextId.load();
    if (!agentContextId)
        return;
    WTF::String message, stack;
    {
        Locker locker { Bun::fatalErrorLock };
        message = Bun::fatalErrorMessage.isolatedCopy();
        stack = Bun::fatalErrorStack.isolatedCopy();
    }
    WTF::String signalName = signal ? WTF::String::fromLatin1(signal) : WTF::String();
    // A profile the agent runs on the exiting thread ends with it, as N|Solid
    // stops the main thread's on exit. The thread can't run requests now.
    Bun::AgentProfile profile;
    if (auto* current = Bun::currentThread)
        profile = Bun::stopAgentProfile(*current->vm);
    {
        Locker locker { Bun::exitLock };
        Bun::exitEventSent = false;
    }
    bool posted = ScriptExecutionContext::postTaskTo(agentContextId, Bun::exitAgentLoopKind.load(), [code, signalName = WTF::move(signalName).isolatedCopy(), message = WTF::move(message), stack = WTF::move(stack), profile = WTF::move(profile)](ScriptExecutionContext& agent) {
        auto* agentGlobal = agent.jsGlobalObject();
        auto& agentVM = agent.vm();
        JSValue listener = Bun::exitListener().get();
        bool called = false;
        if (agentGlobal && listener && listener.isCallable()) {
            auto scope = DECLARE_TOP_EXCEPTION_SCOPE(agentVM);
            MarkedArgumentBuffer arguments;
            arguments.append(jsNumber(code));
            arguments.append(signalName.isNull() ? jsUndefined() : jsString(agentVM, signalName));
            arguments.append(message.isNull() ? jsUndefined() : jsString(agentVM, message));
            arguments.append(stack.isNull() ? jsUndefined() : jsString(agentVM, stack));
            arguments.append(Bun::profileAnswer(agentVM, agentGlobal, profile));
            JSC::call(agentGlobal, listener, arguments, "N|Solid exit"_s);
            called = !scope.exception();
            if (auto* exception = scope.exception()) {
                scope.clearException();
                Zig::GlobalObject::reportUncaughtExceptionAtEventLoop(agentGlobal, exception);
            }
        }
        if (!called) {
            Locker locker { Bun::exitLock };
            Bun::exitEventSent = true;
            Bun::exitCondition.notifyAll();
        }
    });
    if (!posted)
        return;
    Locker locker { Bun::exitLock };
    Bun::exitCondition.waitFor(Bun::exitLock, Bun::EXIT_EVENT_TIMEOUT, [] {
        assertIsHeld(Bun::exitLock);
        return Bun::exitEventSent;
    });
}

// The main thread and each worker, from src/jsc/nsolid_agent.rs, while the
// agent runs.
// On the thread being registered: its loop and GC figures start here.
extern "C" void Bun__NSolid__registerThread(JSC::JSGlobalObject* globalObject, uint32_t contextId)
{
    using namespace Bun;
    auto stats = adoptRef(*new NSolidThreadStats(us_loop_timing_of_current_thread(), us_loop_timing_now_ns()));
    auto& vm = globalObject->vm();
    if (!currentThread) {
        currentThread = new NSolidCurrentThread {
            stats.copyRef(),
            makeUnique<NSolidGCObserver>(vm.heap, stats.copyRef()),
            &vm,
            contextId,
        };
    }
    __atomic_store_n(&us_loop_iteration_hook, loopIteration, __ATOMIC_RELAXED);
    Locker locker { nsolidThreadsLock };
    nsolidThreads().append({ contextId, WTF::emptyString(), WTF::move(stats), &vm });
}

// On the thread being taken off, before its VM goes away.
extern "C" void Bun__NSolid__unregisterThread(uint32_t contextId)
{
    using namespace Bun;
    delete std::exchange(currentThread, nullptr);
    Locker locker { nsolidThreadsLock };
    nsolidThreads().removeFirstMatching([&](auto& thread) { return thread.contextId == contextId; });
}

// A collection that code asked for (`Bun.gc`, `gc()`, `bun:jsc`), counted as
// N|Solid counts V8's forced ones; `full` when it is a synchronous full
// collection, as V8's collect-all-available-garbage ones are.
extern "C" void Bun__NSolid__noteRequestedCollection(bool full)
{
    if (auto* current = Bun::currentThread) {
        current->stats->gcForcedCount.fetch_add(1, std::memory_order_relaxed);
        if (full)
            current->stats->gcFullCount.fetch_add(1, std::memory_order_relaxed);
    }
}

// Called on the N|Solid agent thread (src/jsc/nsolid_agent.rs), in the agent's
// own VM, with the main thread's entry point. The startup marks are
// nanoseconds since the process started. An
// exception is reported and the agent stays idle: it must never take the
// application down.
extern "C" void Bun__NSolid__startAgent(Zig::GlobalObject* globalObject, const BunString* mainPath, double mainEntryNs, double agentThreadNs, double agentVmNs)
{
    auto& vm = JSC::getVM(globalObject);
    auto scope = DECLARE_TOP_EXCEPTION_SCOPE(vm);

    JSValue start = BUN_REQUIRE_INTERNAL_MODULE(globalObject, InternalNsolidAgent, "Startup");
    if (auto* exception = scope.exception()) {
        scope.clearException();
        Zig::GlobalObject::reportUncaughtExceptionAtEventLoop(globalObject, exception);
        return;
    }

    MarkedArgumentBuffer arguments;
    arguments.append(jsString(vm, WTF::String::fromLatin1(Bun::agentId())));
    arguments.append(Bun::toJS(globalObject, *mainPath));
    arguments.append(jsNumber(mainEntryNs));
    arguments.append(jsNumber(agentThreadNs));
    arguments.append(jsNumber(agentVmNs));
    JSC::call(globalObject, start, arguments, "N|Solid agent"_s);
    if (auto* exception = scope.exception()) {
        scope.clearException();
        Zig::GlobalObject::reportUncaughtExceptionAtEventLoop(globalObject, exception);
    }
}

// JSC's shell-timeout trap callback (set in JSCInitialize): the work
// runOnThread queued for this thread.
void Bun__NSolid__onShellTimeoutCheck(JSC::VM& vm)
{
    Bun::runQueuedTasks(vm);
}
