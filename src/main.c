#include <assert.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <threads.h>
#include <unistd.h> // read
#include <fcntl.h> // posix_fadvise
#include <sys/stat.h>
#include <sys/mman.h>

#define KB 1024
#define MB * 1025 * KB
// #define BUFSIZE 160 MB
// #define BUFSIZE 512
// #define BUFSIZE 100
#define BUFSIZE 512 MB
#define NUMWORKER 10
// #define NUMWORKER 1

#define white_space(c) ((c) == ' ' || (c) == '\t')
#define valid_digit(c) ((c) >= '0' && (c) <= '9')

double my_atof(char const* p)
{
    double sign, value;

    sign = 1.0;
    if (*p == '-') {
        sign = -1.0;
        p += 1;
    }

    for (value = 0.0; valid_digit(*p); p += 1) {
        value = value * 10.0 + (*p - '0');
    }

	double pow10 = 10.0;
	p += 1; // skip decimal point
	value += (*p - '0') / pow10;
	pow10 *= 10.0;
	p += 1;

    return sign * value;
}

void *
bsearch(
	void const* key,
	void const* base,
	size_t n,
	size_t size,
	int (*comp)(void const* a, void const* b))
{
	const unsigned char *p = base;
	size_t m ;
	int r;

	while (n > 0) {
		m = (n - 1) >> 1;
		p = (unsigned char const*)base + m * size;
		if ((r = comp(key, p)) < 0) {
			n = m;
		}
		else if (r > 0) {
			base = p + size;
			n -= m + 1;
		}
		else if (m == 0) {
			return (void *)p;
		}
		else {
			n = m + 1;
		}
	}

	return (void *)p;
}

struct weather_data
{
	char city[101];
	double min;
	double max;
	double sum;
	size_t n;
};
int
weather_data_comp(void const* a, void const* b)
{
	struct weather_data* wd = (struct weather_data*)b;
	char const* key = (char const*)a;
	char const* city = wd->city;
	return strcmp(key, city);
}

struct vec_weather_data
{
	struct weather_data* data;
	size_t size;
	size_t cap;
};
void
vec_weather_data_init(struct vec_weather_data* vwd, size_t cap)
{
	vwd->data = malloc(sizeof(struct weather_data)*cap);
	vwd->size = 0;
	vwd->cap = cap;
}
void
vec_weather_data_deinit(struct vec_weather_data* vwd)
{
	// for (size_t i = 0; i < vwd->size; ++i) {
	// 	free(vwd->data[i].city);
	// }
	free(vwd->data);
	vwd->data = 0;
	vwd->size = 0;
	vwd->cap = 0;
}

void
vec_weather_data_json(
		struct vec_weather_data* vwd)
{
	printf("{");
	for (size_t i = 0; i < vwd->size; i++)
	{
		if (i != 0)
		{
			printf(", ");
		}

		struct weather_data* weather_data = &vwd->data[i];
		printf("%s=%.01f/%.01f/%.01f", weather_data->city, weather_data->min, weather_data->sum/weather_data->n, weather_data->max);
	}
	printf("}\n");
}
void
vec_weather_data_pp(
		struct vec_weather_data* vwd)
{
	for (size_t i = 0; i < vwd->size; i++)
	{
		struct weather_data* weather_data = &vwd->data[i];
		printf("> %zu %s: %f %f %f\n", i, weather_data->city, weather_data->min, weather_data->max, weather_data->sum/weather_data->n);
	}
}
struct weather_data*
vec_weather_data_find_or_create(
		struct vec_weather_data* vwd,
		char* key,
		double temp
	)
{
	struct weather_data* p = (struct weather_data*)bsearch(key, vwd->data, vwd->size, sizeof(struct weather_data), weather_data_comp);

	int entry_exists = vwd->size > 0 && strcmp(key, p->city) == 0;
	if (!entry_exists)
	{
		if (vwd->size > 0) {
			if (strcmp(key, p->city) > 0) {
				p = p + 1;
			}
		}
		vwd->size++;
		int is_resize_required = vwd->size >= vwd->cap;
		if (is_resize_required) {
			// TODO: Check if realloc worked
			printf("need to realloc\n");
			vwd->data = realloc(vwd->data, sizeof(struct weather_data)*(vwd->cap*2));
			vwd->cap *= 2;
		}
		memmove(
				(void*)(p+1),
				(void*)p,
				(vwd->size-(p - vwd->data))*sizeof(struct weather_data)
			);
		// p->city = strdup(key);
		stpcpy(p->city, key);
		p->min = temp;
		p->max = temp;
		p->sum = 0;
		p->n = 0;
	}

	return p;
}
struct weather_data*
vec_weather_data_find_or_create2(
		struct vec_weather_data* vwd,
		struct weather_data wd
	)
{
	struct weather_data* p = (struct weather_data*)bsearch(wd.city, vwd->data, vwd->size, sizeof(struct weather_data), weather_data_comp);

	int entry_exists = vwd->size > 0 && strcmp(wd.city, p->city) == 0;
	if (!entry_exists)
	{
		if (vwd->size > 0) {
			if (strcmp(wd.city, p->city) > 0) {
				p = p + 1;
			}
		}
		vwd->size++;
		int is_resize_required = vwd->size >= vwd->cap;
		if (is_resize_required) {
			// TODO: Check if realloc worked
			vwd->data = realloc(vwd->data, sizeof(struct weather_data)*(vwd->cap*2));
			vwd->cap *= 2;
		}
		memmove(
				(void*)(p+1),
				(void*)p,
				(vwd->size-(p - vwd->data))*sizeof(struct weather_data)
			);

		stpcpy(p->city, wd.city);
		p->min = wd.min;
		p->max = wd.max;
		p->sum = wd.sum;
		p->n = wd.n;
	}

	return p;
}
void
vec_weather_data_merge(struct vec_weather_data* vwd_dst, struct vec_weather_data* vwd_src)
{
	for (size_t i = 0; i < vwd_src->size; ++i) 
	{
		struct weather_data* wd_src_ptr = &vwd_src->data[i];
		struct weather_data* wd_dst_ptr = vec_weather_data_find_or_create2(vwd_dst, *wd_src_ptr);
		wd_dst_ptr->min = wd_dst_ptr->min < wd_src_ptr->min ? wd_dst_ptr->min : wd_src_ptr->min;
		wd_dst_ptr->max = wd_dst_ptr->max > wd_src_ptr->max ? wd_dst_ptr->max : wd_src_ptr->max;
		wd_dst_ptr->sum = wd_dst_ptr->sum + wd_src_ptr->sum;
		wd_dst_ptr->n = wd_dst_ptr->n + wd_src_ptr->n;
	}
}

enum WorkerState
{
	WorkerStateIdle = 0,
	WorkerStateWorking = 1,
	WorkerStateKilled = 2, // TODO rename
};
struct worker
{
	thrd_t       thrd;
	atomic_int   state;
	atomic_int   running;
	mtx_t mutex;
	cnd_t cond;
	int number;
	int jobs_executed;
	char*  buffer;
	char*  buffer_end;
	ssize_t      buffer_size; // rename in capacity
	struct vec_weather_data vec_weather_data;

	char const* buffer_it;
};
int worker_run(void*);
void
worker_init(
		struct worker* worker,
		ssize_t buffer_size,
		int number
	)
{
	worker->state = WorkerStateIdle;
	worker->running = 1;
	worker->number = number;
	mtx_init(&worker->mutex, mtx_plain);
	cnd_init(&worker->cond);
	worker->jobs_executed = 0;
	worker->buffer = (void*)malloc(buffer_size+1);
	worker->buffer_size = buffer_size;
	vec_weather_data_init(&worker->vec_weather_data, 50000000);

	thrd_create(&worker->thrd, worker_run, worker);
}
void
worker_deinit(struct worker* worker)
{
	int result = 0;
	thrd_join(worker->thrd, &result);
	vec_weather_data_deinit(&worker->vec_weather_data);
	free((void*)worker->buffer);
	worker->buffer_size = 0;
}
int
worker_run(void* data)
{
	struct worker* worker = data;

	do 
	{
		if (! worker->running)
		{
			goto end_thread;
		}

		mtx_lock(&worker->mutex);
		cnd_wait(&worker->cond, &worker->mutex);

		if (! worker->running)
		{
			mtx_unlock(&worker->mutex);
			goto end_thread;
			break; // does this work?
		}

		if (worker->state == WorkerStateIdle) {
			mtx_unlock(&worker->mutex);
			continue;
		}

		worker->jobs_executed += 1;
		worker->buffer_it = worker->buffer;

		while (worker->buffer_it <= worker->buffer_end-1) {
			char* end_of_line = strchr(worker->buffer_it, '\n');
			if (end_of_line == 0)
			{
				goto foo;
				break;// does not break the while loop???
			}

#if 0
			char* delim_ptr = strchr(worker->buffer_it, ';');
#else
			char* delim_ptr = end_of_line - 4; //strchr(worker->buffer_it, ';');
			while (*delim_ptr != ';') {
				delim_ptr -= 1;
			}
#endif
			ssize_t delim_idx = delim_ptr - worker->buffer_it;

			if (delim_ptr == 0) {
				// TODO issue ?
				break; 
			}

			char city[101];
			// char*  city = strndup(worker->buffer_it, delim_idx);
			strncpy(city, worker->buffer_it, delim_idx); // TODO: ensure this is lower than 101
			city[delim_idx] = 0;
			// stpncpy(city, worker->buffer_it, delim_idx); // TODO: ensure this is lower than 101

			double temp = my_atof(delim_ptr+1);

			struct weather_data* weather_data = vec_weather_data_find_or_create(&worker->vec_weather_data, city, temp);
			weather_data->min = weather_data->min > temp ? temp : weather_data->min;
			weather_data->max = weather_data->max < temp ? temp : weather_data->max;
			weather_data->sum += temp;
			weather_data->n += 1;

			// free(city);

			worker->buffer_it = end_of_line + 1;
		} while(1);

foo:
		// printf("[%d] work completed %zd\n", worker->number, worker->buffer_size);

		worker->state = WorkerStateIdle;
		mtx_unlock(&worker->mutex);
	} while(1);

end_thread:
	worker->state = WorkerStateKilled;

	return 0;
}

int
main(int argc, char** argv)
{
	if (argc < 2)
	{
		// TODO: Print usage
		exit(1);
	}

	int measurements_fd = 0;
	char const* measurements_path = 0;

	measurements_path = argv[1];

	measurements_fd = open(measurements_path, O_RDONLY);
	if (measurements_fd < 0)
	{
		printf("unable to open file '%s': %s\n", measurements_path, strerror(errno));
		exit(1);
	}
	posix_fadvise(measurements_fd, 0, 0, POSIX_FADV_SEQUENTIAL);
	posix_fadvise(measurements_fd, 0, 0, POSIX_FADV_DONTNEED);


	struct worker workers[NUMWORKER] = {0};
	size_t const number_workers = sizeof(workers)/sizeof(struct worker);
	for (size_t i = 0; i < number_workers; ++i)
	{
		// printf("XX i:%zu %lu %lu\n", i, sizeof(workers), sizeof(workers)/sizeof(struct worker));
		worker_init(&workers[i], BUFSIZE, i);
	} 

	char* buffer = 0;
	buffer = malloc(BUFSIZE);
	assert(buffer);
	char* buffer_append_ptr = buffer;
	ssize_t bytes_read = 0;
	// TODO: read needs to write the data read after the data which was moved in front and only the number of bytes left
	while ((bytes_read = read(measurements_fd, (void*)buffer_append_ptr, BUFSIZE-(buffer_append_ptr - buffer)-2)) > 0) 
	{
		buffer_append_ptr[bytes_read] = 0;
		char* last_endline = strrchr(buffer, '\n') + 1;

		for (size_t i = 0; i < number_workers; i = (i + 1) % number_workers)
		{
			if (workers[i].state > WorkerStateIdle)
			{
				continue;
			}
			
			memmove((void*)workers[i].buffer, buffer, last_endline - buffer - 1);
			workers[i].buffer_end = workers[i].buffer + (last_endline - buffer)+1;
			workers[i].buffer_end[1] = 0; // I don't like this, strchr in worker_run needs it to know where to stop
			workers[i].state = WorkerStateWorking;

			mtx_lock(&workers[i].mutex);
			cnd_signal(&workers[i].cond);
			mtx_unlock(&workers[i].mutex);

			break;
		}

		memmove((void*)buffer, last_endline, BUFSIZE - (unsigned int)(last_endline - buffer));
		buffer_append_ptr = buffer + BUFSIZE - (unsigned int)(last_endline - buffer) - 2;
	}

	// Wait till all complete to send exit signal
	for (size_t i = 0; i < number_workers; ++i)
	{
		while (workers[i].state == WorkerStateWorking);
		workers[i].running = 0;
		mtx_lock(&workers[i].mutex);
		cnd_signal(&workers[i].cond);
		mtx_unlock(&workers[i].mutex);
	} 

	// Wait till all exited
	for (size_t i = 0; i < number_workers; ++i)
	{
		while (workers[i].state != WorkerStateKilled);
	} 

	// merge
	for (size_t i = 1; i < number_workers; ++i)
	{
		vec_weather_data_merge(&workers[0].vec_weather_data, &workers[i].vec_weather_data);
	} 

	vec_weather_data_json(&workers[0].vec_weather_data);
	for (size_t i = 0; i < number_workers; ++i)
	{
		worker_deinit(&workers[i]);
	} 

	free((void*)buffer);

	return 0;
}
