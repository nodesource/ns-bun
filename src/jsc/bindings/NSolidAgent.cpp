#include "root.h"
#include "NSolidAgent.h"
#include "ZigGlobalObject.h"
#include "InternalModuleRegistry.h"
#include "ScriptExecutionContext.h"
#include "BunClientData.h"
#include "BunModuleRegistry.h"
#include "headers-handwritten.h"
#include <JavaScriptCore/JSMapIterator.h>
#include <JavaScriptCore/ModuleRegistryEntry.h>
#include <JavaScriptCore/ObjectConstructor.h>
#include <JavaScriptCore/Strong.h>
#include <wtf/Condition.h>
#include <wtf/CryptographicallyRandomNumber.h>
#include <wtf/Lock.h>
#include <wtf/NeverDestroyed.h>
#include <wtf/text/MakeString.h>
#include <mutex>

using namespace JSC;
using WebCore::ScriptExecutionContext;

namespace Bun {

// The threads the agent reports on. A thread's id is its context's identifier
// less one, as `worker.threadId` is: the main thread is 0. As in N|Solid, a
// thread's name is empty until the thread names itself.
struct NSolidThread {
    uint32_t contextId;
    WTF::String name;
};
static Lock nsolidThreadsLock;
static Vector<NSolidThread>& nsolidThreads() WTF_REQUIRES_LOCK(nsolidThreadsLock)
{
    static NeverDestroyed<Vector<NSolidThread>> threads;
    return threads;
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
    {
        Locker locker { Bun::exitLock };
        Bun::exitEventSent = false;
    }
    bool posted = ScriptExecutionContext::postTaskTo(agentContextId, Bun::exitAgentLoopKind.load(), [code, signalName = WTF::move(signalName).isolatedCopy(), message = WTF::move(message), stack = WTF::move(stack)](ScriptExecutionContext& agent) {
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
extern "C" void Bun__NSolid__registerThread(uint32_t contextId)
{
    Locker locker { Bun::nsolidThreadsLock };
    Bun::nsolidThreads().append({ contextId, WTF::emptyString() });
}

extern "C" void Bun__NSolid__unregisterThread(uint32_t contextId)
{
    Locker locker { Bun::nsolidThreadsLock };
    Bun::nsolidThreads().removeFirstMatching([&](auto& thread) { return thread.contextId == contextId; });
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
