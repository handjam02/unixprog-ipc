/*
 * ipcclean.c -- 비정상 종료 뒤 남은 응용 IPC 자원 정리 (make cleanipc)
 * ipc_pipe.c 또는 ipc_shm.c와 링크한다. 실제 정리는 각 구현의 ipc_cleanup()이 한다.
 */
#include <stdio.h>
#include "ipc.h"

#ifndef IPC_NAME
#define IPC_NAME "unknown"
#endif

int main(void)
{
    ipc_cleanup();
    printf("ipcclean[%s]: 정리함\n", IPC_NAME);
    return 0;
}
