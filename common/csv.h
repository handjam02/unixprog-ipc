/*
 * csv.h -- 결과 CSV 열기 (7조)
 *
 * 측정 구간 안에서는 쓰지 않는다(기획서 2장: 결과는 메모리에 쌓았다가 끝난 뒤 저장).
 * 요약 CSV는 여러 프로세스(클라이언트 N개)가 같은 파일에 한 줄씩 덧붙이므로
 * 덧붙이기 모드는 csv_close까지 flock으로 잠가 머리줄이 두 번 들어가지 않게 한다.
 */
#ifndef APP_CSV_H
#define APP_CSV_H

#include <stdio.h>

int   ensure_dir(const char *dir);                   /* mkdir -p. 0 성공 */

/* append = 0: 새로 쓰고 머리줄을 쓴다.
 * append = 1: 덧붙이고, 파일이 비어 있을 때만 머리줄을 쓴다.
 * header는 줄바꿈 없이. 실패하면 NULL(perror 출력) */
FILE *csv_open(const char *path, const char *header, int append);
void  csv_close(FILE *f);

#endif
