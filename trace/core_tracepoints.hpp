// LTTng 双遍 include：CREATE_PROBES 遍会带 TRACEPOINT_HEADER_MULTI_READ 重读本文件，
// 缺这个条件则第二遍 body 被挡掉 → 事件实例表为空 → probe 永不注册（编译仍通过，静默失效）
#if !defined(_CORE_TRACEPOINTS_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define _CORE_TRACEPOINTS_H

#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER fins

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "trace/core_tracepoints.hpp"

#include <lttng/tracepoint.h>

// ==================== 1. 不需要 node_id 的通用线程/系统级事件 ====================

TRACEPOINT_EVENT(
    fins,
    wake,
    TP_ARGS(),
    TP_FIELDS()
)

TRACEPOINT_EVENT(
    fins,
    release,
    TP_ARGS(),
    TP_FIELDS()
)

TRACEPOINT_EVENT(
    fins,
    finished,
    TP_ARGS(),
    TP_FIELDS()
)

TRACEPOINT_EVENT(
    fins,
    sleep,
    TP_ARGS(),
    TP_FIELDS()
)

#endif /* _CORE_TRACEPOINTS_H */

#include <lttng/tracepoint-event.h>