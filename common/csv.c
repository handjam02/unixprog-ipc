/*
 * csv.c -- 결과 CSV 열기 (7조)
 */
#include "csv.h"

#include <errno.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>

/* mkdir -p: 중간 폴더까지 만든다 */
int ensure_dir(const char *dir)
{
    char p[1024];
    size_t n = strlen(dir);

    if (n == 0 || n >= sizeof(p)) return -1;
    memcpy(p, dir, n + 1);
    for (size_t i = 1; i <= n; i++) {
        if (p[i] != '/' && p[i] != '\0') continue;
        char c = p[i];
        p[i] = '\0';
        if (mkdir(p, 0755) < 0 && errno != EEXIST) {
            perror(p);
            return -1;
        }
        p[i] = c;
    }
    return 0;
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
