/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 *
 * NOTE (dual-boot):
 *   This code targets a native Linux install (Ubuntu 22.04 / 24.04).
 *   The environment-check.sh script rejects non-VM environments with
 *   systemd-detect-virt; on a dual-boot machine run the check with
 *   --build-only, or comment out the VIRT != "none" guard.
 *   All kernel features used here (CLONE_NEWPID, CLONE_NEWNS,
 *   CLONE_NEWUTS, namespace file-descriptors) are available on any
 *   stock Ubuntu kernel >= 5.15 and require only CAP_SYS_ADMIN.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* ---------------------------------------------------------------
 * Forward declarations
 * --------------------------------------------------------------- */
static supervisor_ctx_t *g_ctx;   /* set in run_supervisor for signal handlers */

/* Monitor function declarations */
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes);
int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid);

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ===============================================================
 * Bounded buffer helpers
 * =============================================================== */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/* ---------------------------------------------------------------
 * TODO (engine) 1: bounded_buffer_push
 *
 * Policy: block the caller while the buffer is full (classic
 * producer/consumer).  If shutdown begins while we are waiting,
 * return -1 so callers can propagate the drain signal cleanly.
 * --------------------------------------------------------------- */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait until there is room or we are shutting down. */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Copy item into the circular buffer at the tail slot. */
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* Wake at least one consumer. */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ---------------------------------------------------------------
 * TODO (engine) 2: bounded_buffer_pop
 *
 * Returns  0 and fills *item on success.
 * Returns -1 when shutdown is in progress AND the buffer is empty,
 * signalling the consumer thread to drain and exit.
 * --------------------------------------------------------------- */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /*
     * Wait until there is data.  On shutdown we still drain whatever
     * remains before returning the sentinel -1.
     */
    while (buffer->count == 0) {
        if (buffer->shutting_down) {
            pthread_mutex_unlock(&buffer->mutex);
            return -1;
        }
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    /* Dequeue from the head. */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* Wake a blocked producer. */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ---------------------------------------------------------------
 * TODO (engine) 3: logging_thread
 *
 * Continuously pops log_item_t values from the bounded buffer and
 * appends the payload to the per-container log file under LOG_DIR/.
 * Exits cleanly once the buffer signals shutdown-and-empty.
 * --------------------------------------------------------------- */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;
        ssize_t written;
        size_t remaining;
        const char *ptr;

        /* Build the log file path: logs/<container_id>.log */
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR,
                 item.container_id);

        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open");
            continue;
        }

        /* Write the full chunk, retrying on short writes. */
        ptr = item.data;
        remaining = item.length;
        while (remaining > 0) {
            written = write(fd, ptr, remaining);
            if (written < 0) {
                if (errno == EINTR)
                    continue;
                perror("logging_thread: write");
                break;
            }
            ptr += written;
            remaining -= (size_t)written;
        }

        close(fd);
    }

    return NULL;
}

/* ---------------------------------------------------------------
 * TODO (engine) 4: child_fn
 *
 * Entry point for the cloned child process.  Sets up the isolated
 * namespace environment, chroots, mounts /proc, redirects I/O to
 * the log pipe, then execs the requested command.
 * --------------------------------------------------------------- */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    char *argv_exec[4] = { "/bin/sh", "-c", cfg->command, NULL };

    /* Set a distinctive hostname inside the UTS namespace. */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("child_fn: sethostname");
        return 1;
    }

    /* chroot into the container rootfs. */
    if (chroot(cfg->rootfs) < 0) {
        perror("child_fn: chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("child_fn: chdir /");
        return 1;
    }

    /*
     * Mount a fresh procfs so the container sees only its own PID
     * namespace (PID 1 = init inside, host PIDs invisible).
     */
    if (mount("proc", "/proc", "proc",
              MS_NOSUID | MS_NODEV | MS_NOEXEC, NULL) < 0) {
        /* Non-fatal if /proc does not exist in this rootfs. */
        if (errno != ENOENT)
            perror("child_fn: mount /proc (continuing)");
    }

    /* Apply scheduler niceness if requested. */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("child_fn: nice (continuing)");
    }

    /* Redirect stdout and stderr to the log pipe. */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    execvp(argv_exec[0], argv_exec);
    perror("child_fn: execvp");
    return 127;
}

/* ---------------------------------------------------------------
 * Helper: spawn one container from a control_request_t.
 * Allocates the container record, clone()s the child, registers it
 * with the kernel monitor, and starts the log-reader thread.
 * --------------------------------------------------------------- */
typedef struct {
    supervisor_ctx_t *ctx;
    container_record_t *rec;    /* already inserted into ctx->containers */
    int log_read_fd;
} log_reader_args_t;

static void *log_reader_thread(void *arg)
{
    log_reader_args_t *la = (log_reader_args_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, la->rec->id, sizeof(item.container_id) - 1);

    while ((n = read(la->log_read_fd, item.data, sizeof(item.data))) > 0) {
        item.length = (size_t)n;
        if (bounded_buffer_push(&la->ctx->log_buffer, &item) != 0)
            break;
    }

    close(la->log_read_fd);
    free(la);
    return NULL;
}

static int spawn_container(supervisor_ctx_t *ctx, const control_request_t *req)
{
    char *stack;
    pid_t child_pid;
    int pipefd[2];
    child_config_t *cfg;
    container_record_t *rec;
    log_reader_args_t *la;
    pthread_t reader_tid;

    /* Allocate child stack (grows downward on x86-64). */
    stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("spawn_container: malloc stack");
        return -1;
    }

    /* Per-container config passed into the clone child. */
    cfg = malloc(sizeof(*cfg));
    if (!cfg) {
        free(stack);
        return -1;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id,      req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs,  req->rootfs,       sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command,      sizeof(cfg->command) - 1);
    cfg->nice_value = req->nice_value;

    /* Create the log pipe: child writes, supervisor reads. */
    if (pipe(pipefd) < 0) {
        perror("spawn_container: pipe");
        free(cfg);
        free(stack);
        return -1;
    }
    cfg->log_write_fd = pipefd[1];

    /*
     * Ensure LOG_DIR exists before the logger tries to open files there.
     */
    mkdir(LOG_DIR, 0755);

    child_pid = clone(child_fn,
                      stack + STACK_SIZE,   /* top of stack */
                      CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD,
                      cfg);

    /* Close write end in parent; child has its own copy after clone. */
    close(pipefd[1]);

    if (child_pid < 0) {
        perror("spawn_container: clone");
        close(pipefd[0]);
        free(cfg);
        free(stack);
        return -1;
    }

    /* Build the container_record. */
    rec = calloc(1, sizeof(*rec));
    if (!rec) {
        close(pipefd[0]);
        free(cfg);
        free(stack);
        return -1;
    }
    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->host_pid          = child_pid;
    rec->started_at        = time(NULL);
    rec->state             = CONTAINER_RUNNING;
    rec->soft_limit_bytes  = req->soft_limit_bytes;
    rec->hard_limit_bytes  = req->hard_limit_bytes;
    snprintf(rec->log_path, sizeof(rec->log_path),
             "%s/%s.log", LOG_DIR, rec->id);

    /* Insert into the supervisor's container list (prepend). */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Register with the kernel monitor module. */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, rec->id, child_pid,
                              req->soft_limit_bytes, req->hard_limit_bytes);

    /* Start a per-container log-reader thread. */
    la = malloc(sizeof(*la));
    if (la) {
        la->ctx = ctx;
        la->rec = rec;
        la->log_read_fd = pipefd[0];
        if (pthread_create(&reader_tid, NULL, log_reader_thread, la) != 0) {
            close(pipefd[0]);
            free(la);
        } else {
            pthread_detach(reader_tid);
        }
    } else {
        close(pipefd[0]);
    }

    free(stack);
    free(cfg);
    return 0;
}

/* ---------------------------------------------------------------
 * Provided ioctl wrappers
 * --------------------------------------------------------------- */
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ---------------------------------------------------------------
 * Signal handler for the supervisor process.
 *
 * SIGCHLD: reap a child and update the container_record state.
 * SIGINT / SIGTERM: trigger graceful shutdown.
 * --------------------------------------------------------------- */
static void supervisor_sigchld(int sig)
{
    (void)sig;
    /* Actual reaping happens in the event loop with waitpid(). */
}

static void supervisor_sigterm(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* ---------------------------------------------------------------
 * Helper: accept one client connection, read one request, write
 * one response, close the connection socket.
 * --------------------------------------------------------------- */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t n;

    memset(&resp, 0, sizeof(resp));

    n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "bad request length");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {

    case CMD_START:
    case CMD_RUN: {
        if (spawn_container(ctx, &req) < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "failed to start container %s", req.container_id);
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "started %s", req.container_id);
        }
        break;
    }

    case CMD_PS: {
        container_record_t *r;
        int offset = 0;

        resp.status = 0;
        pthread_mutex_lock(&ctx->metadata_lock);
        for (r = ctx->containers; r && offset < (int)sizeof(resp.message) - 1; r = r->next) {
            offset += snprintf(resp.message + offset,
                               sizeof(resp.message) - (size_t)offset,
                               "%s\t%s\tpid=%d\n",
                               r->id,
                               state_to_string(r->state),
                               r->host_pid);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        break;
    }

    case CMD_LOGS: {
        /*
         * Stream log file contents back over the control socket.
         * A real implementation would use a dedicated streaming
         * channel; here we fit what we can in the message field.
         */
        char path[PATH_MAX];
        int log_fd;
        char buf[sizeof(resp.message)];
        ssize_t rn;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);
        log_fd = open(path, O_RDONLY);
        if (log_fd < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "no log for container %s", req.container_id);
        } else {
            resp.status = 0;
            rn = read(log_fd, buf, sizeof(buf) - 1);
            if (rn > 0) {
                buf[rn] = '\0';
                strncpy(resp.message, buf, sizeof(resp.message) - 1);
            }
            close(log_fd);
        }
        break;
    }

    case CMD_STOP: {
        container_record_t *r;

        pthread_mutex_lock(&ctx->metadata_lock);
        for (r = ctx->containers; r; r = r->next) {
            if (strncmp(r->id, req.container_id, sizeof(r->id)) == 0) {
                if (r->state == CONTAINER_RUNNING) {
                    kill(r->host_pid, SIGTERM);
                    r->state = CONTAINER_STOPPED;
                    if (ctx->monitor_fd >= 0)
                        unregister_from_monitor(ctx->monitor_fd,
                                                r->id, r->host_pid);
                }
                break;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = (r != NULL) ? 0 : -1;
        snprintf(resp.message, sizeof(resp.message),
                 r ? "stopped %s" : "unknown container %s", req.container_id);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "unknown command");
        break;
    }

    send(client_fd, &resp, sizeof(resp), 0);
}

/* ---------------------------------------------------------------
 * TODO (engine) 5: run_supervisor  (inner implementation)
 * --------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    (void)rootfs;   /* base rootfs reserved for future pivot_root use */

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* 1. Open the kernel monitor device. */
    ctx.monitor_fd = open("/dev/", DEVICE_NAME, O_RDWR);
    if (ctx.monitor_fd < 0)
        perror("run_supervisor: open /dev/container_monitor (continuing without monitor)");

    /* 2. Create and bind the UNIX domain control socket. */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("run_supervisor: socket");
        goto cleanup;
    }

    unlink(CONTROL_PATH);   /* remove stale socket from a previous run */
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("run_supervisor: bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 8) < 0) {
        perror("run_supervisor: listen");
        goto cleanup;
    }

    /* 3. Install signal handlers. */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_sigchld;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = supervisor_sigterm;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 4. Start the logging consumer thread. */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("run_supervisor: pthread_create logger");
        goto cleanup;
    }

    fprintf(stderr, "Supervisor started. Listening on %s\n", CONTROL_PATH);

    /* 5. Event loop: accept client connections and reap children. */
    while (!ctx.should_stop) {
        int client_fd;
        fd_set rfds;
        struct timeval tv;

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        tv.tv_sec  = 1;
        tv.tv_usec = 0;

        rc = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (rc < 0) {
            if (errno == EINTR)
                goto reap;
            perror("run_supervisor: select");
            break;
        }

        if (rc > 0 && FD_ISSET(ctx.server_fd, &rfds)) {
            client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                handle_client(&ctx, client_fd);
                close(client_fd);
            }
        }

    reap:
        /* Reap all terminated children without blocking. */
        {
            int status;
            pid_t wpid;
            while ((wpid = waitpid(-1, &status, WNOHANG)) > 0) {
                container_record_t *r;
                pthread_mutex_lock(&ctx.metadata_lock);
                for (r = ctx.containers; r; r = r->next) {
                    if (r->host_pid == wpid) {
                        if (WIFEXITED(status)) {
                            r->state     = CONTAINER_EXITED;
                            r->exit_code = WEXITSTATUS(status);
                        } else if (WIFSIGNALED(status)) {
                            r->state       = CONTAINER_KILLED;
                            r->exit_signal = WTERMSIG(status);
                        }
                        if (ctx.monitor_fd >= 0)
                            unregister_from_monitor(ctx.monitor_fd,
                                                    r->id, r->host_pid);
                        break;
                    }
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
            }
        }
    }

    fprintf(stderr, "Supervisor shutting down.\n");

cleanup:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);

    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);

    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free container records. */
    {
        container_record_t *r = ctx.containers, *tmp;
        while (r) {
            tmp = r->next;
            free(r);
            r = tmp;
        }
    }

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
    }

    pthread_mutex_destroy(&ctx.metadata_lock);
    g_ctx = NULL;
    return 0;
}

/* ---------------------------------------------------------------
 * TODO (engine) 6: send_control_request
 *
 * Client side: connect to the supervisor's UNIX domain socket,
 * send the request struct, read back the response, print it.
 * --------------------------------------------------------------- */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t n;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("send_control_request: socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("send_control_request: connect (is the supervisor running?)");
        close(fd);
        return 1;
    }

    /* Send the full request struct. */
    n = send(fd, req, sizeof(*req), 0);
    if (n != (ssize_t)sizeof(*req)) {
        perror("send_control_request: send");
        close(fd);
        return 1;
    }

    /* Read the response. */
    n = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
    close(fd);

    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "send_control_request: short read from supervisor\n");
        return 1;
    }

    /* Print the supervisor's reply. */
    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);

    return (resp.status == 0) ? 0 : 1;
}

/* ---------------------------------------------------------------
 * CLI command implementations
 * --------------------------------------------------------------- */
static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO (engine) 7: ps rendering
     *
     * The supervisor responds with a compact text table:
     *   <id>  <state>  pid=<N>
     * one line per container.  We print it directly since the
     * full detail is already formatted server-side in handle_client().
     */
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
