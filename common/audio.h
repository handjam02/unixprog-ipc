/*
 * audio.h -- 청크 분석, 정규화, 시험용 PCM 생성 (7조)
 *
 * PCM: int16_t, 모노, 48kHz (기획서 5.1절). 값은 최대 진폭(32768) 대비 비율 0..1로 다룬다.
 * 무음 기준과 정규화 목표값은 기획서에 숫자가 없어 여기서 정했다(보고서 4장에 적을 것).
 */
#ifndef APP_AUDIO_H
#define APP_AUDIO_H

#include <stdint.h>
#include <stddef.h>

#define AUDIO_RATE          48000
#define AUDIO_SILENT_THRESH 0.01     /* |x| < 1% (약 -40 dBFS)면 무음 샘플 */
#define AUDIO_SILENT_MIN    480      /* 무음 샘플이 10ms 이상 이어지면 무음 구간 1개 */
#define AUDIO_NORM_TARGET   0.9      /* 정규화 뒤 피크 */

typedef struct {
    double   rms;
    double   peak;
    uint32_t silent_runs;
} audio_stats_t;

/* 청크 하나 분석. 무음 구간은 청크 안에서만 센다(청크 경계에 걸친 구간은 나뉨) */
void audio_analyze(const int16_t *x, size_t n, audio_stats_t *out);

/* 피크가 AUDIO_NORM_TARGET이 되게 제자리에서 키우거나 줄인다. peak는 audio_analyze 값 */
void audio_normalize(int16_t *x, size_t n, double peak);

/* 시험용 신호: 소리 0.4초 + 무음 0.1초 반복, 소리는 사인파 + 잡음, 크기가 천천히 변한다.
 * start는 전체 신호에서 이 청크가 시작하는 샘플 번호, seed는 클라이언트마다 다르게 */
void audio_synth(int16_t *x, size_t n, uint64_t start, uint32_t seed);

#endif
