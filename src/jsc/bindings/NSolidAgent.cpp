#include "root.h"
#include "NSolidAgent.h"
#include "ZigGlobalObject.h"
#include "InternalModuleRegistry.h"
#include "headers-handwritten.h"
#include <wtf/CryptographicallyRandomNumber.h>
#include <mutex>

using namespace JSC;

namespace Bun {

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
