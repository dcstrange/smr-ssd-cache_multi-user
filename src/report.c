#include "string.h"
#include "report.h"
#include "statusDef.h"
#include "unistd.h"
FILE* LogFile;
void info(char* str)
{
    printf("process [%d]: %s\n",getpid(),str);
}

void error(char* str)
{
    printf("process [%d]: %s\n",getpid(),str);
    exit(1);
}

int OpenLogFile(const char* filepath)
{
    LogFile = fopen(filepath,"w+");
    if(LogFile == NULL)
        return errno;
    return 0;
}

int CloseLogFile()
{
    return fclose(LogFile);
}

int WriteLog(char* log)
{
#ifdef LOG_ALLOW
    return fwrite(log,strlen(log),1,LogFile);
#endif // LOG_ALLOW
}
