# unixprog-ipc (7조)

pipe vs System V shared memory: 오디오 청크 분석 서버

클라이언트가 PCM 청크를 보내면 서버가 RMS, 피크, 무음 구간 수를 계산해 돌려준다(정규화 모드는 고친 오디오 전체를 돌려줌). 같은 `server.c` / `client.c`에 IPC 구현(`ipc_pipe.c` 또는 `ipc_shm.c`)을 붙여 두 버전을 만들고 성능을 비교한다.

## 빠른 시작

```bash
make app_pipe                       # 또는 make app_shm, make (전부)
app/server_pipe                     # 터미널 1, Ctrl+C로 종료
app/client_pipe -s 64K -m stats     # 터미널 2
make cleanipc                       # 비정상 종료 뒤 남은 IPC 자원 정리
```

## 구성

| 경로 | 내용 |
|---|---|
| `common/` | 공통 모듈: 분석(audio), 공유 통계(stats), 작업 큐(queue), 시간(timing), CSV |
| `app/` | `ipc.h`, `ipc_pipe.c`, `ipc_shm.c`, `server.c`, `client.c` |
| `phase1/` | 1단계 IPC 기초 성능 벤치마크와 측정 결과 |
| `predictions/` | 성능 예측 원문 (구현 전 작성) |

빌드 타깃, 실행 옵션, 결과 파일, 측정 재현, 자원 정리는 [`readme.txt`](readme.txt)에 있다.
