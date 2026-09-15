// LTTng 双遍 include：CREATE_PROBES 遍会带 TRACEPOINT_HEADER_MULTI_READ 重读本文件，
// 缺这个条件则第二遍 body 被挡掉 → 事件实例表为空 → probe 永不注册（编译仍通过，静默失效）
#if !defined(_ALGO_TRACEPOINTS_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define _ALGO_TRACEPOINTS_H

#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER algo

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "trace/algo_tracepoints.hpp"

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(
    algo,                    // Provider 名称
    execute,           // 事件名称
    TP_ARGS(
        const char*, node_id // 输入参数
    ),
    TP_FIELDS(
        ctf_string(node_id, node_id) // 输出字段（多项时不用逗号分隔）
    )
)

TRACEPOINT_EVENT(
    algo,
    complete,
    TP_ARGS(const char*, node_id),
    TP_FIELDS(
        ctf_string(node_id, node_id)
    )
)

TRACEPOINT_EVENT(
    algo,
    working,
    TP_ARGS(const char*, node_id, int, core_id, long long, seg_us),
    TP_FIELDS(
        ctf_string(node_id, node_id)
        ctf_integer(int, core_id, core_id)
        ctf_integer(long long, seg_us, seg_us)
    )
)

#endif /* _ALGO_TRACEPOINTS_H */

#include <lttng/tracepoint-event.h>