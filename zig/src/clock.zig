const std = @import("std");

export fn fast_now_ms() i64 {
    const ts = std.posix.clock_gettime(.REALTIME_COARSE) catch return std.time.milliTimestamp();
    return @divTrunc(ts.sec * 1000 + @divTrunc(ts.nsec, 1_000_000), 1);
}
