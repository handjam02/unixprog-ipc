/*
 * audio.c -- 청크 분석, 정규화, 시험용 PCM 생성 (7조)
 */
#include "audio.h"

#include <math.h>

void audio_analyze(const int16_t *x, size_t n, audio_stats_t *out)
{
    const int thresh = (int)(AUDIO_SILENT_THRESH * 32768.0);
    double sumsq = 0;
    int peak = 0;
    size_t run = 0;
    uint32_t runs = 0;

    for (size_t i = 0; i < n; i++) {
        int v = x[i];
        int a = v < 0 ? -v : v;
        sumsq += (double)v * v;
        if (a > peak) peak = a;
        if (a < thresh) {
            if (++run == AUDIO_SILENT_MIN) runs++;   /* 기준 길이에 닿는 순간 1번만 센다 */
        } else {
            run = 0;
        }
    }
    out->rms = n ? sqrt(sumsq / (double)n) / 32768.0 : 0;
    out->peak = peak / 32768.0;
    out->silent_runs = runs;
}

void audio_normalize(int16_t *x, size_t n, double peak)
{
    double g;

    if (peak <= 0) return;             /* 전부 0이면 그대로 */
    g = AUDIO_NORM_TARGET / peak;
    for (size_t i = 0; i < n; i++) {
        double v = x[i] * g;
        if (v > 32767) v = 32767;
        if (v < -32768) v = -32768;
        x[i] = (int16_t)lrint(v);
    }
}

void audio_synth(int16_t *x, size_t n, uint64_t start, uint32_t seed)
{
    const uint64_t period = AUDIO_RATE / 2;          /* 0.5초 */
    const uint64_t sound = period * 4 / 5;           /* 그중 0.4초 소리 */
    const double freq = 220.0 + (seed % 8) * 55.0;   /* 클라이언트마다 다른 음 */
    uint32_t r = seed * 2654435761u + (uint32_t)start;

    for (size_t i = 0; i < n; i++) {
        uint64_t t = start + i;
        double v = 0;
        if (t % period < sound) {
            double sec = (double)t / AUDIO_RATE;
            double amp = 0.3 + 0.2 * sin(2 * M_PI * 0.2 * sec);     /* 5초 주기로 크기 변화 */
            r = r * 1664525u + 1013904223u;
            v = amp * sin(2 * M_PI * freq * sec) + 0.02 * ((double)(r >> 8) / (1u << 24) - 0.5);
        }
        x[i] = (int16_t)lrint(v * 32767.0);
    }
}
