#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>

#include "float_vec.h"
#include "barrier.h"
#include "utils.h"

// Compares the inputs as floats.
int cmpfunc (const void * a, const void * b) {

	return (*(const float *) a > *(const float *) b)
			- (*(const float *) a < *(const float *) b); 
}

// Quicksort for the floats struct
void
qsort_floats(floats* xs)
{
	qsort(xs->data, xs->size, sizeof(float), cmpfunc);
}

// Determine if the array contains the given element
int contains(floats* array, float element) {

	for (int i = 0; i < array->size; i++) {
		if (array->data[i] == element)
			return 1;
	}
	return 0;
}

// Samples the input data for P processes
floats*
sample(float* data, long size, int P)
{

	int sample_size = 3*(P-1);
	floats* sample_array = make_floats(0);

	float element = data[rand() % size];	

	for (int i = 0; i < sample_size; i++) {
		if (contains(sample_array, element))
			i--;
		else {
			floats_push(sample_array, element);		
		}

		element = data[rand() % size];	
	}

	qsort_floats(sample_array);

	floats* samples = make_floats(0);
	floats_push(samples, 0);

	for (int i = 1; i < sample_size; i+=3) {
		floats_push(samples, sample_array->data[i]);
	}

	floats_push(samples, 1000);
	free_floats(sample_array);
	return samples;
}

// Sorts the data for the current process
void
sort_worker(int pnum, float* data, long size, int P, floats* samps, long* sizes, barrier* bb)
{
    floats* xs = make_floats(0);

	for (int i = 0; i < size; i++) {
		if (data[i] >= samps->data[pnum] && data[i] < samps->data[pnum + 1]) {
			floats_push(xs, data[i]);
		}
	}

	sizes[pnum] = xs->size;
	
    printf("%d: start %.04f, count %ld\n", pnum, samps->data[pnum], xs->size);

    qsort_floats(xs);

	barrier_wait(bb);

	int start = 0;
	for (int i = 0; i <= pnum - 1; i++) {
		start += sizes[i];
	}

	int end = 0;
	for (int i = 0; i <= pnum; i++) {
		end += sizes[i];
	}

	for (int i = start; i < end; i++) {
		data[i] = xs->data[i - start];
	}


    free_floats(xs);
}

// Spawns all child processes and sorts
void
run_sort_workers(float* data, long size, int P, floats* samps, long* sizes, barrier* bb)
{
    pid_t kids[P];
    (void) kids; // suppress unused warning

	for (int i = 0; i < P; i++) {
		if ((kids[i] = fork())) {

		}
		else {
			sort_worker(i, data, size, P, samps, sizes, bb);
			exit(0);
		}
	}

    for (int ii = 0; ii < P; ++ii) {
        int rv = waitpid(kids[ii], 0, 0);
        check_rv(rv);
    }
}

// Samples the data, and sorts using P processes
void
sample_sort(float* data, long size, int P, long* sizes, barrier* bb)
{
    floats* samps = sample(data, size, P);
    run_sort_workers(data, size, P, samps, sizes, bb);

    free_floats(samps);
}

// Implementation of Parallel Sample Sort
int
main(int argc, char* argv[])
{
    alarm(120);

    if (argc != 3) {
        printf("Usage:\n");
        printf("\t%s P data.dat\n", argv[0]);
        return 1;
    }

    const int P = atoi(argv[1]);
    const char* fname = argv[2];

    seed_rng();

    int rv;
    struct stat st;
    rv = stat(fname, &st);
    check_rv(rv);

    const int fsize = st.st_size;
    if (fsize < 8) {
        printf("File too small.\n");
        return 1;
    }

    int fd = open(fname, O_RDWR);
    check_rv(fd);

    void* file = mmap(NULL, fsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    (void) file; // suppress unused warning.

    long count = ((long*) file)[0];
    float* data = ((float*) file) + 2;

    long sizes_bytes = P * sizeof(long);
	long* sizes = mmap(NULL, P, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    barrier* bb = make_barrier(P);

    sample_sort(data, count, P, sizes, bb);

    free_barrier(bb);

	munmap(file, fsize);
	munmap(sizes, sizes_bytes);

    return 0;
}

