#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

struct weather_data
{
	char* city;
	double min;
	double max;
	double avg;
};

struct vec_weather_data
{
	struct weather_data* data;
	size_t size;
	size_t cap;
};
void vec_weather_data_init(struct vec_weather_data* vwd, size_t cap)
{
	vwd->data = malloc(sizeof(struct weather_data)*cap);
	vwd->size = 0;
	vwd->cap = cap;
}
void vec_weather_data_deinit(struct vec_weather_data* vwd)
{
	for (size_t i = 0; i < vwd->size; ++i) {
		free(vwd->data[i].city);
	}
	free(vwd->data);
	vwd->data = 0;
	vwd->size = 0;
	vwd->cap = 0;
}
void
vec_weather_data_pp(
		struct vec_weather_data* vwd)
{
	for (size_t i = 0; i < vwd->size; i++)
	{
		struct weather_data* weather_data = &vwd->data[i];
		printf("> %zu %s: %f %f %f\n", i, weather_data->city, weather_data->min, weather_data->max, weather_data->avg);
	}
}
struct weather_data*
vec_weather_data_find_or_create(
		struct vec_weather_data* vwd,
		char* key,
		double temp
	)
{
	ssize_t l = 0;
	ssize_t r = vwd->size;

	while ((r - l) > 1) // TODO: This seems to make issues 
	{
		ssize_t m = (l + r) / 2;

		int cmp = strcmp(key, vwd->data[m].city);
		if (cmp == 0)
		{
			return &vwd->data[m];
		}

		if (cmp < 0)
		{
			r = m;
		} else if (cmp > 0) {
			l = m;
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
			(void*)&vwd->data[l+1],
			(void*)&vwd->data[l],
			sizeof(struct weather_data)*(vwd->size-l)
		);
	vwd->data[l].city = strdup(key);
	vwd->data[l].min = temp;
	vwd->data[l].max = temp;
	vwd->data[l].avg = temp;
	return &vwd->data[l];
}

int
main(int argc, char** argv)
{
	if (argc < 2) {
		return -1;
	}

	char const* file_path = argv[1];
	FILE *file_ptr = fopen(file_path, "r");
	if (file_ptr == 0) {
		printf("Unable to open file %s. Error: %s\n", file_path, strerror(errno));
		return -1;
	}


	struct vec_weather_data vec_weather_data;
	vec_weather_data_init(&vec_weather_data, 1024);

	size_t n = 0;
	char *buffer = 0;
	do {
		ssize_t read = getline(&buffer, &n, file_ptr);
		if (read == -1) {
			break;
		}
		if (n == 0 || buffer[0] == '#') {
			continue;
		}

		char* delim_ptr = strchr(buffer, ';');
		ssize_t delim_idx = delim_ptr - buffer;

		char*  city = strndup(buffer, delim_idx);
		double temp = atof(delim_ptr+1);

		struct weather_data* weather_data = vec_weather_data_find_or_create(&vec_weather_data, city, temp);
		weather_data->min = weather_data->min > temp ? temp : weather_data->min;
		weather_data->max = weather_data->max < temp ? temp : weather_data->max;
		weather_data->avg = (weather_data->avg + temp) / 2;

		// vec_weather_data_pp(&vec_weather_data);
		// printf("\n");
		free(city);
	} while (1);

	vec_weather_data_pp(&vec_weather_data);
	vec_weather_data_deinit(&vec_weather_data);

	if (fclose(file_ptr) != 0) {
		printf("Unable to close file %s. Error: %s\n", file_path, strerror(errno));
		return -1;
	}

	return 0;
}
