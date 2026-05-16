#include <stdint.h>
#include <time.h>

int64_t fast_now_ms(void) {
    struct timespec ts;
#ifdef __linux__
    clock_gettime(CLOCK_REALTIME_COARSE, &ts);
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}
