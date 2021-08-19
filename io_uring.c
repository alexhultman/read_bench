/* Minimal io_uring/pipe benchmark for small messages */

#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>

#include "liburing.h"

#define MAX_BATCH 2000

#define BUF_SIZE 1024
struct iovec buffer;

#define MAX_PIPES 10000
int *pipes;
int num_pipes = 0;

int main(int argc, char **argv) {

    /* We have a static roof */
    pipes = malloc(sizeof(int) * 2 * MAX_PIPES);

    /* Pipes is first arg */
    sscanf(argv[1], "%d", &num_pipes);
    if (num_pipes > MAX_PIPES) {
        printf("Pipes: %d, exceeds MAX_PIPES: %d\n", num_pipes, MAX_PIPES);
        exit(1);
    }
    printf("Pipes: %d\n", num_pipes);
    for (int i = 0; i < num_pipes; i++) {
        if (pipe2(&pipes[i * 2], O_NONBLOCK | O_CLOEXEC)) {
            printf("Cannot create pipes!\n");
            return 0;
        }
    }

    /* Create an io_uring */
    struct io_uring ring;
    // Since we have two events per rotation, an io_ring twice the size of the number of pipes makes sense
    if (io_uring_queue_init(num_pipes * 2, &ring, IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF)) {
        printf("Cannot create an io_uring!\n");
        return 0;
    }

    /* Register our pipes with io_uring */
    if (io_uring_register_files(&ring, pipes, num_pipes * 2)) {
        printf("Cannot register files with io_uring!\n");
        return 0;
    }

    /* Create and register buffer with io_uring */
    buffer.iov_base = malloc(BUF_SIZE * 2 * num_pipes);
    buffer.iov_len = BUF_SIZE * 2 * num_pipes;
    memset(buffer.iov_base, 'R', BUF_SIZE * 2 * num_pipes);

    if (io_uring_register_buffers(&ring, &buffer, 1)) {
        printf("Cannot register buffer with io_uring!\n");
        return 0;
    }

    struct io_uring_sqe *sqe;
    struct io_uring_cqe **cqes = malloc(sizeof(struct io_uring_cqe *) * MAX_BATCH);

    /* Start time */
    clock_t start = clock();

    for (int i = 0; i < 10000; i++) {

        for (int j = 0; j < num_pipes; j++) {
            /* Prepare a write */
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                printf("Cannot allocate sqe!\n");
                return 0;
            }

            io_uring_prep_write_fixed(sqe, j * 2 + 1, buffer.iov_base + BUF_SIZE * (j * 2 + 1), BUF_SIZE, 0, 0);
            sqe->flags |= IOSQE_FIXED_FILE;

            /* Prepare a read */
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                printf("Cannot allocate sqe!\n");
                return 0;
            }
            io_uring_prep_read_fixed(sqe, j * 2, buffer.iov_base + BUF_SIZE * j * 2, BUF_SIZE, 0, 0);
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        /* Submit */
        for (int j = 0; j < num_pipes * 2; j += io_uring_submit(&ring));

        /* Poll for completions */
        int remaining_completions = num_pipes * 2;
        while (remaining_completions) {

            /* Poll until done in batches */
            int completions = io_uring_peek_batch_cqe(&ring, cqes, MAX_BATCH);
            if (completions == 0) {
                continue;
            }

            /* Check and mark every completion */
            for (int j = 0; j < completions; j++) {

                /* Check for failire */
                if (cqes[j]->res < 0) {
                    printf("Async task failed: %s\n", strerror(-cqes[j]->res));
                    return 0;
                }

                /* Did we really read/write everything? */
                if (cqes[j]->res != BUF_SIZE) {
                    printf("Mismatching read/write: %d\n", cqes[j]->res);
                    return 0;
                }
            }
            
            /* Does the same as io_uring_cqe_seen (that literally does io_uring_cq_advance(&ring, 1)), except all at once */
            io_uring_cq_advance(&ring, completions);

            /* Update */
            remaining_completions -= completions;
        }
    }

    /* Print time */
    clock_t end = clock();
    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("Time: %f\n", cpu_time_used);

    /* Append this run */
    FILE *runs = fopen("io_uring_runs", "a");
    fprintf(runs, "%f\n", cpu_time_used);
    fclose(runs);
}
