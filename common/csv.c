/*
 * csv.c -- 결과 CSV 열기 (7조)
 */
#include "csv.h"

#include <errno.h>
#include <sys/file.h>
#include <sys/stat.h>

int ensure_dir(const char *dir)
{
    if (mkdir(dir, 0755) == 0 || errno == EEXIST)
        return 0;
    perror(dir);
    return -1;
}

FILE *csv_open(const char *path, const char *header, int append)
{
    FILE *f = fopen(path, append ? "a" : "w");

    if (!f) { perror(path); return NULL; }
    if (append) {
        flock(fileno(f), LOCK_EX);
        fseek(f, 0, SEEK_END);
    }
    if (ftell(f) == 0)
        fprintf(f, "%s\n", header);
    return f;
}

void csv_close(FILE *f)
{
    if (!f) return;
    fflush(f);
    flock(fileno(f), LOCK_UN);
    fclose(f);
}
