from cffi import FFI

ffibuilder = FFI()
ffibuilder.cdef('''
enum status {
   ...
};


typedef int... time_t;
typedef int... suseconds_t;

struct timeval {
    time_t tv_sec;
    suseconds_t tv_usec;
    ...;
};

struct ifstats {
    char name[...];
    enum status status;
    unsigned int ibytes;
    unsigned int obytes;
    struct timeval timestamp;
};


typedef unsigned int u_int;
struct ifstats get_stats(int row);

int sysctl(
    const int *name, u_int namelen, void *oldp, size_t *oldlenp,
    const void *newp, size_t newlen);

int sysctlbyname(
    const char *name, void *oldp, size_t *oldlenp, const void *newp, size_t newlen);

int sysctlnametomib(const char *name, int *mibp, size_t *sizep);

int get_if_limit(void);

    ''')
ffibuilder.set_source(
    'pfstatsd._ifstats',
    r'''
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <net/if.h>
#include <net/if_mib.h>

enum status {
    SUCCESSFUL,
    FAILED_SYSCTL,
    FAILED_ROW_TOO_SMALL,
    FAILED_ROW_TOO_HIGH
};

struct ifstats {
    char name[IFNAMSIZ];
    enum status status;
    unsigned int ibytes;
    unsigned int obytes;
    struct timeval timestamp;
};

int get_if_limit(void) {
    int name[5] = {
        CTL_NET,
        PF_LINK,
        NETLINK_GENERIC,
        IFMIB_SYSTEM,
        IFMIB_IFCOUNT
    };
    int result = -1;
    size_t len = sizeof(result);
    if(sysctl(name, 5, &result, &len, (void*)0, 0) == -1) {
        perror("sysctl_call_for_limit:");
        return -1;
    }
    return result;
};

struct ifstats get_stats(int row) {
       struct ifmibdata ifmd = { };
       struct ifstats stats = { };
       struct timeval tv;

       int name[6];
       size_t len;

       if (row < 1) {
           stats.status = FAILED_ROW_TOO_SMALL;
           return stats;
       }
       int max_rows = get_if_limit();
       if (row > max_rows) {
           stats.status = FAILED_ROW_TOO_HIGH;
           return stats;
       }
       name[0] = CTL_NET;
       name[1] = PF_LINK;
       name[2] = NETLINK_GENERIC;
       name[3] = IFMIB_IFDATA;
       name[4] = row;
       name[5] = IFDATA_GENERAL;

       len = sizeof(&ifmd);

       (void) gettimeofday(&tv, NULL);
       if (sysctl(name, 6, &ifmd, &len, (void *)0, 0) == -1) {
           perror("sysctl");
           stats.status = FAILED_SYSCTL;
           return stats;
       }

       stats.status = SUCCESSFUL;
       stats.timestamp.tv_sec = tv.tv_sec;
       stats.timestamp.tv_usec = tv.tv_usec;
       stats.ibytes = ifmd.ifmd_data.ifi_ibytes;
       stats.obytes = ifmd.ifmd_data.ifi_obytes;
       strncpy(stats.name, ifmd.ifmd_name, IFNAMSIZ);
       return stats;
};
    ''')

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
