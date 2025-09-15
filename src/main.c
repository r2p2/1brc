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
#include <stdint.h>

#define KB 1024
#define MB * 1025 * KB
// #define NUMWORKER 11
#define NUMWORKER 10
// #define NUMWORKER 1
#define BUCKETS 1000

#define valid_digit(c) ((c) >= '0' && (c) <= '9')

// TODO remove me
mtx_t mtx;
cnd_t cnd;

void
str_pp(char* s, size_t offset, size_t n)
{
	for (size_t i = 0; i < n; ++i)
	{
		char c = s[offset + i];
		char cn = c == '\n' ? '#' : c;
		printf("%c", cn);
	}
	printf("\n");
}

struct buffer
{
	char* data;
	size_t size;
	size_t cap;
};
void
buffer_init(struct buffer* buffer, size_t cap)
{
	buffer->data = calloc(cap, sizeof(char));
	assert(buffer->data);
	buffer->size = 0;
	buffer->cap = cap;
}
size_t
buffer_space_left(struct buffer* buffer)
{
	return buffer->cap - buffer->size;
}
size_t
buffer_push_back(struct buffer* buffer, char* src, size_t n)
{
	size_t space_left = buffer->cap - buffer->size;
	size_t bytes_to_copy = n > space_left ? space_left : n;

	memcpy(buffer->data + buffer->size, src, bytes_to_copy);
	buffer->size += bytes_to_copy;
	
	return bytes_to_copy;
}
size_t
buffer_pop_front(struct buffer* buffer, char* dst, size_t n)
{
	size_t bytes_to_copy = buffer->size < n ? buffer->size : n;

	memcpy(dst, buffer->data, bytes_to_copy);
	memmove(buffer->data, buffer->data + bytes_to_copy, buffer->cap - bytes_to_copy);

	buffer->size = buffer->size - bytes_to_copy;
	
	return bytes_to_copy;
}
size_t
buffer_drop_front(struct buffer* buffer, size_t n)
{
	size_t bytes_to_copy = buffer->size < n ? buffer->size : n;

	memmove(buffer->data, buffer->data + bytes_to_copy, buffer->cap - bytes_to_copy);

	buffer->size = buffer->size - bytes_to_copy;
	
	return bytes_to_copy;
}
void
buffer_pp(struct buffer* buffer)
{
	for (size_t i = 0; i < buffer->size; ++i)
	{
		char c = buffer->data[i];
		if (c == '\n')
		{
			c = '#';
		}
		printf("%c", c);
	}
	printf("\n");
}

struct vec
{
	char** data;
	size_t size;
	size_t cap;
};
void
vec_init(struct vec* v, size_t cap)
{
	v->data = calloc(cap, sizeof(char*));
	v->size = 0;
	v->cap = cap;
}
void
vec_deinit(struct vec* v)
{
	free(v->data);
	v->size = 0;
	v->cap = 0;
}
void
vec_push_back(
		struct vec* v,
		char* value
	)
{
	if (v->size == v->cap) {
		v->data = realloc(v->data, sizeof(char*)*v->cap*2);
		v->cap = v->cap * 2;
	}

	v->data[v->size] = value;
	v->size = v->size + 1;
}

double my_atof(char const* p, char const* dot)
{
	double value = 0.0;
	int sign = 0;
	if (*p == '-')
	{
		sign = 1;
		++p;
	}
    for (; p != dot; p += 1) {
        value = value * 10.0 + (*p - '0');
    }

	value += (dot[1] - '0') / 10.0;
	return sign ? -value : value;
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
	struct weather_data* next;
};
void weather_data_init(struct weather_data* wd)
{
	wd->city[0] = 0;
	wd->min = 0;
	wd->max = 0;
	wd->sum = 0;
	wd->n = 0;
	wd->next = 0;
}

int
str_comp(void const* a, void const* b)
{
	return strcmp(*(char const**)a, *(char const**)b);
}











unsigned long
hash(char *s)
{
	unsigned char* str = (unsigned char*)s;

    unsigned long hash = 5381;
    int c;

    while ((c = *str++))
	{
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
	}

    return hash;
}


struct hashmap_weather_data
{
	struct weather_data** data;
	size_t buckets;
};
void
hashmap_weather_data_init(struct hashmap_weather_data* hm, size_t buckets)
{
	hm->data = calloc(buckets, sizeof(struct weather_data*));
	assert(hm->data);
	hm->buckets = buckets;
}
void
hashmap_weather_data_deinit(struct hashmap_weather_data* hm)
{
	// TODO: Walk through map
	free(hm->data);
	hm->data = 0;
	hm->buckets = 0;
}
struct weather_data*
hashmap_weather_data_find_or_add(struct hashmap_weather_data* hm, char* city)
{
	ssize_t idx = hash(city) % hm->buckets;
	struct weather_data* wd = hm->data[idx]; 

	while (wd)
	{
		if (strcmp(wd->city, city) == 0)
		{
			return wd;
		}

		wd = wd->next;
	}

	wd = calloc(1, sizeof(struct weather_data));
	strcpy(wd->city, city);
	wd->next = hm->data[idx];
	hm->data[idx] = wd;

	return wd;
}
struct weather_data*
hashmap_weather_data_find(struct hashmap_weather_data* hm, char* city)
{
	ssize_t idx = hash(city) % hm->buckets;
	struct weather_data* wd = hm->data[idx]; 

	while (wd)
	{
		if (strcmp(wd->city, city) == 0)
		{
			return wd;
		}

		wd = wd->next;
	}

	return 0;
}
void
hashmap_weather_data_merge(
		struct hashmap_weather_data* dst,
		struct hashmap_weather_data* src
	)
{
	for (size_t i = 0; i < src->buckets; ++i)
	{
		struct weather_data* wd_src = src->data[i];
		while (wd_src)
		{
			struct weather_data* wd_dst = hashmap_weather_data_find_or_add(dst, wd_src->city);
			wd_dst->min = wd_dst->min < wd_src->min ? wd_dst->min : wd_src->min;
			wd_dst->max = wd_dst->max > wd_src->max ? wd_dst->max : wd_src->max;
			wd_dst->sum = wd_dst->sum + wd_src->sum;
			wd_dst->n = wd_dst->n + wd_src->n;
			
			wd_src = wd_src->next;
		}
	}
}
void
hashmap_weather_data_stats(
		struct hashmap_weather_data* hm
	)
{
	for (size_t i = 0; i < hm->buckets; ++i)
	{
		int count = 0;
		struct weather_data* wd_src = hm->data[i];
		while (wd_src)
		{
			++count;
			wd_src = wd_src->next;
		}
		printf(">%zu: %d\n", i, count);
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
	atomic_int   running;
	int number;
	FILE* file;
	size_t file_chunk_size;
	char* write_ahead_buffer;
	size_t write_ahead_buffer_size;
	size_t write_ahead_buffer_cap;
	atomic_int write_ahead_state; // 0 init; 1 request; 2 complete
	struct hashmap_weather_data weather_data;
};
int worker_run(void*);
void
worker_init(
		struct worker* worker,
		size_t buckets,
		int number,
		char const* path,
		size_t file_chunk_size,
		size_t buffer_size
	)
{
	worker->running = 1;
	worker->number = number;
	worker->file_chunk_size = file_chunk_size;
	worker->write_ahead_buffer = calloc(buffer_size, sizeof(char));
	worker->write_ahead_buffer_size = 0;
	worker->write_ahead_buffer_cap = buffer_size;
	worker->write_ahead_state = 0;
	worker->file = fopen(path, "r");
	fseek(worker->file, worker->number * worker->file_chunk_size, SEEK_SET);
	hashmap_weather_data_init(&worker->weather_data, buckets);

	thrd_create(&worker->thrd, worker_run, worker);
}
void
worker_deinit(struct worker* worker)
{
	fclose(worker->file);
	int result = 0;
	thrd_join(worker->thrd, &result);
	hashmap_weather_data_deinit(&worker->weather_data);
}
int
worker_run(void* data)
{
	struct worker* worker = data;

	struct buffer buffer;
	buffer_init(&buffer, worker->write_ahead_buffer_cap * 2);

	char* read_buffer = calloc(worker->write_ahead_buffer_cap, sizeof(char));
	size_t bytes_read = fread(read_buffer, sizeof(char), worker->write_ahead_buffer_cap, worker->file);
	buffer_push_back(&buffer, read_buffer, bytes_read);
	// free(read_buffer);


	char* it = strchr(buffer.data, '\n')+1;

	size_t bytes_processed = 0;
	while(1) 
	{
		// printf("%d (%f)\n", worker->number, (double)bytes_processed/worker->file_chunk_size*100);
		// buffer_push_back(&buffer, worker->write_ahead_buffer, bytes_read);
		worker->write_ahead_state = 1;
		// printf("worker: %d preload request\n", worker->number);
		// printf("beginning\n");
		// str_pp(buffer.data, 0, 30);

		while(1)
		{
			if ((size_t)((it - buffer.data)) >= buffer.size)
			{
				break;
			}
			// char* end_of_line = strchr(it, '\n');
			char* end_of_line = memchr(it, '\n', buffer.size - (it - buffer.data));
			if (end_of_line == 0) {
				// printf("end %ld\n", it - buffer.data);
				// str_pp(buffer.data, it-buffer.data-10, buffer.size - (it-buffer.data)+10);
				break;
			}
			size_t line_len = end_of_line - it + 1;

			char* delim_ptr = strchr(it, ';');
			// char* delim_ptr = line + len - 4; //strchr(worker->buffer_it, ';');
			// while (*delim_ptr != ';') {
			// 	delim_ptr -= 1;
			// }
		
			ssize_t delim_idx = delim_ptr - it;

			if (delim_ptr == 0) {
				// TODO issue ?
				// printf("foo\n");
				break; 
			}

			char city[101];
			strncpy(city, it, delim_idx); // TODO: ensure this is lower than 101
			city[delim_idx] = 0;

			double temp = my_atof(delim_ptr+1, end_of_line-2);

			struct weather_data* weather_data = hashmap_weather_data_find_or_add(&worker->weather_data, city);
			// TODO: There might be an issue with the initial state
			weather_data->min = weather_data->min > temp ? temp : weather_data->min;
			weather_data->max = weather_data->max < temp ? temp : weather_data->max;
			weather_data->sum += temp;
			weather_data->n += 1;

			it += line_len;
			// buffer_drop_front(&buffer, line_len);
			bytes_processed += line_len;
			if (bytes_processed > worker->file_chunk_size) {
				goto done;
			}
		}
		buffer_drop_front(&buffer, it - buffer.data);
		while (worker->write_ahead_state != 2) ;
		if (worker->write_ahead_buffer_size == 0) {
			break;
		}
		buffer_push_back(&buffer, worker->write_ahead_buffer, worker->write_ahead_buffer_size);
		it = buffer.data;
	}
done:

	worker->running = 0;
	// printf("worker: %d end\n", worker->number);

	// mtx_lock(&mtx);
	// cnd_signal(&cnd);
	// mtx_unlock(&mtx);

	return 0;
}

void merge(struct worker* workers, size_t n)
{
	for (size_t i = 1; i < n; ++i)
	{
		hashmap_weather_data_merge(&workers[0].weather_data, &workers[i].weather_data);
	} 
}

void print(struct worker* worker)
{
	struct vec cities;
	vec_init(&cities, 1024);

	for (size_t i = 0; i < worker->weather_data.buckets; ++i)
	{
		struct weather_data* wd = worker->weather_data.data[i];
		while (wd)
		{
			vec_push_back(&cities, wd->city);
			wd = wd->next;
		}
	}

	qsort(cities.data, cities.size, sizeof(char*), str_comp);

	printf("{");
	for (size_t i = 0; i < cities.size; ++i)
	{
		struct weather_data* wd = hashmap_weather_data_find(&worker->weather_data, cities.data[i]);
		if (wd == 0)
		{
			continue;
		}
		if (i != 0)
		{
			printf(", ");
		}
		printf("%s=%.01f/%.01f/%.01f", wd->city, wd->min, wd->sum/wd->n, wd->max);
	}
	printf("}\n");

}

int
main(int argc, char** argv)
{
	mtx_init(&mtx, mtx_plain);
	cnd_init(&cnd);

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

	FILE* fd = fopen(measurements_path, "r");
	fseek(fd, 0L, SEEK_END);
	long file_size = ftell(fd);
	long chunk_size = file_size / NUMWORKER;
	fclose(fd);

	// printf("file size:%ld %ld\n", file_size, chunk_size);
	
	// posix_fadvise(measurements_fd, 0, 0, POSIX_FADV_DONTNEED);


	struct worker workers[NUMWORKER] = {0};
	size_t const number_workers = sizeof(workers)/sizeof(struct worker);
	for (size_t i = 0; i < number_workers; ++i)
	{
		// printf("XX i:%zu %lu %lu\n", i, sizeof(workers), sizeof(workers)/sizeof(struct worker));
		// worker_init(&workers[i], BUCKETS, i, measurements_path, chunk_size, 500 MB);
		// worker_init(&workers[i], BUCKETS, i, measurements_path, chunk_size, 200 MB);
		// worker_init(&workers[i], BUCKETS, i, measurements_path, chunk_size, 100 MB);
		worker_init(&workers[i], BUCKETS, i, measurements_path, chunk_size, 90 MB);
		// worker_init(&workers[i], BUCKETS, i, measurements_path, chunk_size, 80 MB);
		// worker_init(&workers[i], BUCKETS, i, measurements_path, chunk_size, 50 MB);
		// worker_init(&workers[i], BUCKETS, i, measurements_path, chunk_size, 1 MB);
	} 


	int workers_running = 0;
	do
	{
		workers_running = 0;
		for (size_t i = 0; i < NUMWORKER; ++i)
		{
			struct worker* worker = &workers[i];
			if (worker->running == 0) {
				continue;
			}
			workers_running += 1;
			if (workers[i].write_ahead_state == 1) {
				// printf("worker: %zu preload start %zu\n", i, worker->write_ahead_buffer_cap);
				worker->write_ahead_buffer_size = fread(worker->write_ahead_buffer, sizeof(char), worker->write_ahead_buffer_cap, worker->file);
				// printf("worker: %zu preload complete\n", i);
				worker->write_ahead_state = 2;
			}
		} 
	} while(workers_running);


	// int workers_left = NUMWORKER;
	// while (workers_left > 0)
	// {
	// 	mtx_lock(&mtx);
	// 	cnd_wait(&cnd, &mtx);
	// 	mtx_unlock(&mtx);
	// 	--workers_left;
	// }

	merge(workers, number_workers);
	// for (size_t i = 1; i < number_workers; ++i)
	// {
	// 	hashmap_weather_data_merge(&workers[0].weather_data, &workers[i].weather_data);
	// } 

	print(&workers[0]);
	// struct vec cities;
	// vec_init(&cities, 1024);
	//
	// for (size_t i = 0; i < workers[0].weather_data.buckets; ++i)
	// {
	// 	struct weather_data* wd = workers[0].weather_data.data[i];
	// 	while (wd)
	// 	{
	// 		vec_push_back(&cities, wd->city);
	// 		wd = wd->next;
	// 	}
	// }
	//
	// qsort(cities.data, cities.size, sizeof(char*), str_comp);
	//
	// printf("{");
	// for (size_t i = 0; i < cities.size; ++i)
	// {
	// 	struct weather_data* wd = hashmap_weather_data_find(&workers[0].weather_data, cities.data[i]);
	// 	if (wd == 0)
	// 	{
	// 		continue;
	// 	}
	// 	if (i != 0)
	// 	{
	// 		printf(", ");
	// 	}
	// 	printf("%s=%.01f/%.01f/%.01f", wd->city, wd->min, wd->sum/wd->n, wd->max);
	// }
	// printf("}\n");

	return 0;
}
