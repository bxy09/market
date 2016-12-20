#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <wchar.h>
#include <iconv.h>

#include "TDFAPI.h"
#include "TDFAPIStruct.h"
#include "reader.h"

#define DUMPALL


#define MIN(x, y) ((x)>(y)?(y):(x))

void RecvData(THANDLE hTdf, TDF_MSG* pMsgHead);

extern "C" void GoRecvIdx(char * name, char status, int date,  int time, int close, int preClose, int open, int high, int low, int volume);
extern "C" void GoRecvMkt(char * name, char status, int date, int time, int close, int preClose, int open, int high, int low, int volume);
extern "C" void GoRecvDisconnect();

void RecvSys(THANDLE hTdf, TDF_MSG* pSysMsg);

void DumpScreenMarket(TDF_MARKET_DATA* pMarket, int nItems);
void DumpScreenIndex(TDF_INDEX_DATA* pIndex, int nItems);

#define ELEM_COUNT(arr) (sizeof(arr)/sizeof(arr[0]))
#define SAFE_STR(str) ((str)?(str):"")
#define SAFE_CHAR(ch) ((ch) ? (ch) : ' ')


int code_convert(char *from_charset,char *to_charset,char *inbuf,size_t* inlen,char *outbuf,size_t* outlen)
{
        iconv_t cd;
        int rc;
        char **pin = &inbuf;
        char **pout = &outbuf;

        cd = iconv_open(to_charset,from_charset);
        if (cd==0)
                return -1;
        memset(outbuf,0,*outlen);
        if (iconv(cd,pin,(size_t*)inlen,pout,(size_t*)outlen) == -1)
                return -1;
        iconv_close(cd);
        return 0;
}
char* chararr2str(char* szBuf, int nBufLen, char arr[], int n)
{
    int nOffset = 0;
    for (int i=0; i<n; i++)
    {
        nOffset += snprintf(szBuf+nOffset, nBufLen-nOffset, "%d(%c) ", arr[i], SAFE_CHAR(arr[i]));
    }
    return szBuf;
}

char* intarr2str(char* szBuf, int nBufLen, int arr[], int n)
{
    int nOffset = 0;
    for (int i=0; i<n; i++)
    {
        nOffset += snprintf(szBuf+nOffset, nBufLen-nOffset, "%d ", arr[i]);
    }
    return szBuf;
}

int main(int argc, char*argv[]) {
    if (argc != 5){
        printf("Usage: program ip port user password\n");
	    exit(1);
    }

    THANDLE hTDF = trace(argv[1],argv[2],argv[3],argv[4]);

    // Press any key to exit
    getchar();

    TDF_Close(hTDF);
}

void close_handler(THANDLE handler) {
    TDF_Close(handler);
}


THANDLE trace(const char* ip, const char* port, const char* user, const char* password)  {
    //TDF_SetEnv(TDF_ENVIRON_HEART_BEAT_INTERVAL, 10);
    //TDF_SetEnv(TDF_ENVIRON_MISSED_BEART_COUNT, 2);
    TDF_SetEnv(TDF_ENVIRON_OUT_LOG, 1);

    TDF_OPEN_SETTING settings = {0};
    strcpy(settings.szIp,   ip);
    strcpy(settings.szPort, port);
    strcpy(settings.szUser, user);
    strcpy(settings.szPwd,  password);
    settings.nReconnectCount = 99999999;
    settings.nReconnectGap = 5;
    settings.pfnMsgHandler = RecvData; //设置数据消息回调函数
    settings.pfnSysMsgNotify = RecvSys;//设置系统消息回调函数
    settings.nProtocol = 0;
    settings.szMarkets = "SZ;SH;";      //需要订阅的市场列表
    settings.szSubScriptions = "";    //需要订阅的股票,为空则订阅全市场
    settings.nDate = 0;//请求的日期，格式YYMMDD，为0则请求今墍
    settings.nTime = 0;//请求的时间，格式HHMMSS，为0则请求实时行情，䶿xffffffff从头请求
    settings.nTypeFlags = DATA_TYPE_ALL; //请求的品种。DATA_TYPE_ALL请求所有品祍
    TDF_ERR nErr = TDF_ERR_SUCCESS;
    THANDLE hTDF = NULL;
    hTDF = TDF_Open(&settings, &nErr);

    if (hTDF==NULL) {
        printf("TDF_Open return error: %d\n", nErr);
        return NULL;
    }
    return hTDF;
}


#define GETRECORD(pBase, TYPE, nIndex) ((TYPE*)((char*)(pBase) + sizeof(TYPE)*(nIndex)))

void RecvData(THANDLE hTdf, TDF_MSG* pMsgHead)
{
   if (!pMsgHead->pData)
   {
       assert(0);
       return ;
   }

   unsigned int nItemCount = pMsgHead->pAppHead->nItemCount;
   unsigned int nItemSize = pMsgHead->pAppHead->nItemSize;

   if (!nItemCount)
   {
       assert(0);
       return ;
   }

   switch(pMsgHead->nDataType)
   {
   case MSG_DATA_MARKET:
       {
           unsigned int preclose = ((TDF_MARKET_DATA*)pMsgHead)->nPreClose;
           if (preclose < 1) {
               break;
           }
           assert(nItemSize == sizeof(TDF_MARKET_DATA));
           DumpScreenMarket((TDF_MARKET_DATA*)pMsgHead->pData, nItemCount);
       }
       break;
   case MSG_DATA_FUTURE:
       break;

   case MSG_DATA_INDEX:
       {
           DumpScreenIndex((TDF_INDEX_DATA*)pMsgHead->pData, nItemCount);
       }
       break;
   case MSG_DATA_TRANSACTION:
       break;
   case MSG_DATA_ORDERQUEUE:
       break;
   case MSG_DATA_ORDER:
       break;
   default:
       {
           assert(0);
       }
       break;
   }
}

void RecvSys(THANDLE hTdf, TDF_MSG* pSysMsg)
{
    if (!pSysMsg ||! hTdf)
    {
        return;
    }

    switch (pSysMsg->nDataType)
    {
    case MSG_SYS_DISCONNECT_NETWORK:
        {
            printf("网络断开\n");
            GoRecvDisconnect();
        }
        break;
    case MSG_SYS_CONNECT_RESULT:
        {
            TDF_CONNECT_RESULT* pConnResult = (TDF_CONNECT_RESULT*)pSysMsg->pData;
            if (pConnResult && pConnResult->nConnResult)
            {
                printf("连接 %s:%s user:%s, password:%s 成功!\n", pConnResult->szIp, pConnResult->szPort, pConnResult->szUser, pConnResult->szPwd);
            }
            else
            {
                printf("连接 %s:%s user:%s, password:%s 失败!\n", pConnResult->szIp, pConnResult->szPort, pConnResult->szUser, pConnResult->szPwd);
            }
        }
        break;
    case MSG_SYS_LOGIN_RESULT:
        {
            TDF_LOGIN_RESULT* pLoginResult = (TDF_LOGIN_RESULT*)pSysMsg->pData;
            //convert gb2312 to utf-8
            char utf_info[128];
            size_t len2 =128;
            size_t len1 = strlen(pLoginResult->szInfo);
            code_convert("gb2312","utf-8",pLoginResult->szInfo,&len1,utf_info,&len2);
            if (pLoginResult && pLoginResult->nLoginResult)
            {
                printf("登陆成功！info:%s, nMarkets:%d\n", utf_info , pLoginResult->nMarkets);
                for (int i=0; i<pLoginResult->nMarkets; i++)
                {
                    printf("market:%s, dyn_date:%d\n", pLoginResult->szMarket[i], pLoginResult->nDynDate[i]);
                }
            }
            else
            {
                printf("登陆失败，原因：%s\n", utf_info);
            }
        }
        break;
    case MSG_SYS_CODETABLE_RESULT:
        break;
    case MSG_SYS_QUOTATIONDATE_CHANGE:
        {
            TDF_QUOTATIONDATE_CHANGE* pChange = (TDF_QUOTATIONDATE_CHANGE*)pSysMsg->pData;
            if (pChange)
            {
                printf("收到行情日期变更通知，即将自动重连！交易所：%s, 原日期:%d, 新日期：%d\n", pChange->szMarket, pChange->nOldDate, pChange->nNewDate);
            }
        }
        break;
    case MSG_SYS_MARKET_CLOSE:
        {
            TDF_MARKET_CLOSE* pCloseInfo = (TDF_MARKET_CLOSE*)pSysMsg->pData;
            if (pCloseInfo)
            {
                printf("闭市消息:market:%s, time:%d, info:%s\n", pCloseInfo->szMarket, pCloseInfo->nTime, pCloseInfo->chInfo);
            }
        }
        break;
    case MSG_SYS_HEART_BEAT:
        {
            printf("收到心跳消息\n");
        }
        break;
    default:
        assert(0);
        break;
    }
}

void DumpScreenMarket(TDF_MARKET_DATA* pMarket, int nItems)
{
#ifdef DUMPALL
    char szBuf1[512];
    char szBuf2[512];
    char szBuf3[512];
    char szBuf4[512];
    char szBufSmall[64];
    for (int i=0; i<nItems; i++)
    {
        const TDF_MARKET_DATA& marketData = pMarket[i];
        GoRecvMkt((char *)marketData.szWindCode,
            marketData.nStatus,
            marketData.nActionDay,
            marketData.nTime,
            marketData.nMatch,
            marketData.nPreClose,
            marketData.nOpen,
            marketData.nHigh,
            marketData.nLow,
            marketData.iVolume
        );
//        printf("万得代码 szWindCode: %s\n", marketData.szWindCode);
//        printf("原始代码 szCode: %s\n", marketData.szCode);
//        printf("业务发生日(自然日) nActionDay: %d\n", marketData.nActionDay);
//        printf("交易日 nTradingDay: %d\n", marketData.nTradingDay);
//        printf("时间(HHMMSSmmm) nTime: %d\n", marketData.nTime);
//        printf("状态 nStatus: %d(%c)\n", marketData.nStatus, SAFE_CHAR(marketData.nStatus));
//        printf("前收盘价 nPreClose: %d\n", marketData.nPreClose);
//        printf("开盘价 nOpen: %d\n", marketData.nOpen);
//        printf("最高价 nHigh: %d\n", marketData.nHigh);
//        printf("最低价 nLow: %d\n", marketData.nLow);
//        printf("最新价 nMatch: %d\n", marketData.nMatch);
//        //printf("申卖价 nAskPrice: %s \n", intarr2str(szBuf1, sizeof(szBuf1), (int*)marketData.nAskPrice, ELEM_COUNT(marketData.nAskPrice)));
//
//        //printf("申卖量 nAskVol: %s \n", intarr2str(szBuf2, sizeof(szBuf2), (int*)marketData.nAskVol, ELEM_COUNT(marketData.nAskVol)));
//
//        //printf("申买价 nBidPrice: %s \n", intarr2str(szBuf3, sizeof(szBuf3), (int*)marketData.nBidPrice, ELEM_COUNT(marketData.nBidPrice)));
//
//        //printf("申买量 nBidVol: %s \n", intarr2str(szBuf4, sizeof(szBuf4), (int*)marketData.nBidVol, ELEM_COUNT(marketData.nBidVol)));
//
//        printf("成交笔数 nNumTrades: %d\n", marketData.nNumTrades);
//
//        printf("成交总量 iVolume: %d\n", marketData.iVolume);
//        printf("成交总金额 iTurnover: %d\n", marketData.iTurnover);
//        //printf("委托买入总量 nTotalBidVol: %I64d\n", marketData.nTotalBidVol);
//        //printf("委托卖出总量 nTotalAskVol: %I64d\n", marketData.nTotalAskVol);
//
//        //printf("加权平均委买价格 nWeightedAvgBidPrice: %u\n", marketData.nWeightedAvgBidPrice);
//        //printf("加权平均委卖价格 nWeightedAvgAskPrice: %u\n", marketData.nWeightedAvgAskPrice);
//
//        //printf("IOPV净值估便nIOPV: %d\n",  marketData.nIOPV);
//        //printf("到期收益率 nYieldToMaturity: %d\n", marketData.nYieldToMaturity);
//        //printf("涨停价 nHighLimited: %d\n", marketData.nHighLimited);
//        //printf("跌停价 nLowLimited: %d\n", marketData.nLowLimited);
//        printf("证券信息前缀 chPrefix: %s\n", chararr2str(szBufSmall, sizeof(szBufSmall), (char*)marketData.chPrefix, ELEM_COUNT(marketData.chPrefix)));
//        //printf("市盈率1 nSyl1: %d\n", marketData.nSyl1);
//        //printf("市盈率1 nSyl2: %d\n", marketData.nSyl2);
//        printf("升跌2（对比上一笔） nSD2: %d\n", marketData.nSD2);
    }

#endif
}

void DumpScreenIndex(TDF_INDEX_DATA* pIndex, int nItems)
{
#ifdef DUMPALL

    for (int i=0; i<nItems; i++)
    {
        const TDF_INDEX_DATA& indexData = pIndex[i];
        GoRecvIdx((char *)indexData.szWindCode,
            'O',
            indexData.nActionDay,
            indexData.nTime,
            indexData.nLastIndex,
            indexData.nPreCloseIndex,
            indexData.nOpenIndex,
            indexData.nHighIndex,
            indexData.nLowIndex,
            indexData.iTotalVolume
        );
//        printf("万得代码 szWindCode: %s\n", indexData.szWindCode);
//        printf("原始代码 szCode: %s\n", indexData.szCode);
//        printf("业务发生日(自然日) nActionDay: %d\n", indexData.nActionDay);
//        printf("交易日 nTradingDay: %d\n", indexData.nTradingDay);
//        printf("时间(HHMMSSmmm) nTime: %d\n", indexData.nTime);
//
//        printf("今开盘指数 nOpenIndex: %d\n", indexData.nOpenIndex);
//        printf("最高指数 nHighIndex: %d\n", indexData.nHighIndex);
//        printf("最低指数 nLowIndex: %d\n", indexData.nLowIndex);
//        printf("最新指数 nLastIndex: %d\n", indexData.nLastIndex);
//        printf("成交总量 iTotalVolume: %d\n", indexData.iTotalVolume);
//        printf("成交总金额 iTurnover: %d\n", indexData.iTurnover);
//        printf("前盘指数 nPreCloseIndex: %d\n", indexData.nPreCloseIndex);

//        if (nItems>1)
//        {
//            printf("\n");
//        }
    }
#endif
}

