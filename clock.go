package snowflake

/*
#cgo LDFLAGS: -L${SRCDIR}/zig/zig-out/lib -lclock_zig
#include <stdint.h>
int64_t fast_now_ms(void);
*/
import "C"

func clockNowMilliseconds() int64 {
	return int64(C.fast_now_ms())
}
