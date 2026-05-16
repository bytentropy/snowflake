package snowflake

/*
#include <stdint.h>

int64_t fast_now_ms(void);
*/
import "C"

func clockNowMilliseconds() int64 {
	return int64(C.fast_now_ms())
}
