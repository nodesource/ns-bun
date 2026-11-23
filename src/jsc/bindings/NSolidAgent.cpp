#include "root.h"
#include "ZigGlobalObject.h"
#include "InternalModuleRegistry.h"
#include "headers-handwritten.h"

using namespace JSC;

// Called on the N|Solid agent thread (src/jsc/nsolid_agent.rs), in the agent's
// own VM. An exception is reported and the agent stays idle: it must never
// take the application down.
extern "C" void Bun__NSolid__startAgent(Zig::GlobalObject* globalObject, const BunString* agentId)
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
    arguments.append(Bun::toJS(globalObject, *agentId));
    JSC::call(globalObject, start, arguments, "N|Solid agent"_s);
    if (auto* exception = scope.exception()) {
        scope.clearException();
        Zig::GlobalObject::reportUncaughtExceptionAtEventLoop(globalObject, exception);
    }
}
