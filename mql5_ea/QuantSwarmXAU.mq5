//+------------------------------------------------------------------+
//| QUANT-SWARM-XAU Broker Adapter v4.00                             |
//| Authenticated flat NDJSON; broker events are authoritative       |
//| Native MetaEditor compilation remains a mandatory release gate   |
//+------------------------------------------------------------------+
#property copyright "QUANT-SWARM-XAU"
#property version   "4.00"
#property strict

#include <Trade\Trade.mqh>
#include <Trade\PositionInfo.mqh>

#define QSX_PROTOCOL_VERSION "4.0"
#define QSX_CACHE_LIMIT 512

input string InpHost                 = "127.0.0.1";
input int    InpPort                 = 9876;
input string InpAuthToken            = "";       // Required; never logged
input string InpConfigHash           = "UNCONFIGURED";
input int    InpMagicNumber          = 202412;
input int    InpSlippagePoints       = 30;
input double InpMaxRiskPct           = 1.0;      // Equal/stricter EA ceiling
input double InpDailyMaxDrawPct      = 3.0;
input int    InpMaxOwnedPositions    = 1;
input int    InpReconnectSec         = 5;
input int    InpMaxFrameBytes        = 16384;
input int    InpMaxCommandRetries    = 3;
input int    InpDealReplayLookbackDays = 90;
input bool   InpPermitRealAccount    = false;    // Requires server approval too
input bool   InpEmergencyStop        = false;
input string InpApproveBaselineUtcDay = "";  // Exact UTC day YYYY-MM-DD; one-day scope
input double InpApproveBaselineEquity = 0.0; // Must match the existing persisted baseline
input bool   InpSendHistoryOnConnect = true;
input int    InpHistoryM1Bars        = 300;
input int    InpHistoryM5Bars        = 300;
input int    InpHistoryM15Bars       = 200;
input int    InpHistoryH1Bars        = 200;
input int    InpHistoryH4Bars        = 120;
input int    InpHistoryD1Bars        = 60;

CTrade        g_Trade;
CPositionInfo g_PositionInfo;

int    g_Socket          = INVALID_HANDLE;
bool   g_Connected       = false;
bool   g_Authenticated   = false;
int    g_ReconnectTicks  = 0;
string g_ReceiveBuffer   = "";
string g_ConnectionEpoch = "";
long   g_SendSequence    = 0;
long   g_LastRecvSequence = 0;

string g_RiskDay         = "";
double g_DayStartEquity  = 0.0;
double g_DailyPnL        = 0.0;
bool   g_DailyHalted     = false;
bool   g_BaselineVerified = false;
bool   g_LocalStateHealthy = true;
long   g_LastDealCursorMs = 0;
ulong  g_LastDealCursorDeal = 0;
datetime g_LastSentM1Bar    = 0;
datetime g_LastSentM5Bar    = 0;
datetime g_LastSentM15Bar   = 0;
datetime g_LastSentH1Bar    = 0;
datetime g_LastSentH4Bar    = 0;
datetime g_LastSentD1Bar    = 0;

struct CommandRecord
{
   string command_id;
   string trade_id;
   string action;
   string direction;
   string status;
   string reason;
   uint   retcode;
   string order_id;
   string deal_id;
   string position_id;
   string position_ticket;
   double actual_price;
   double actual_volume;
   long   expected_position_version;
   double target_sl;
   double final_tp;
};

CommandRecord g_CommandCache[];
CommandRecord g_ActiveCommand;
bool          g_HasActiveCommand = false;
int           g_CommandWritesSinceCompact = 0;

//+------------------------------------------------------------------+
//| Basic encoding and identity helpers                              |
//+------------------------------------------------------------------+
string JsonEscape(string value)
{
   StringReplace(value, "\\", "\\\\");
   StringReplace(value, "\"", "\\\"");
   StringReplace(value, "\r", "\\r");
   StringReplace(value, "\n", "\\n");
   StringReplace(value, "\t", "\\t");
   return value;
}

string J(const string value)
{
   return "\"" + JsonEscape(value) + "\"";
}

string UlongText(const ulong value)
{
   return StringFormat("%I64u", value);
}

string LongText(const long value)
{
   return StringFormat("%I64d", value);
}

long UtcNowMs()
{
   static long baseUtcMs = 0;
   static ulong baseMicroseconds = 0;
   static long lastResult = 0;
   long wallMs = ((long)TimeGMT()) * 1000;
   ulong monotonic = GetMicrosecondCount();
   if(baseUtcMs == 0 || monotonic < baseMicroseconds)
   {
      baseUtcMs = wallMs;
      baseMicroseconds = monotonic;
   }
   long candidate = baseUtcMs + (long)((monotonic - baseMicroseconds) / 1000);
   if(MathAbs((double)(candidate - wallMs)) > 1500.0)
   {
      baseUtcMs = wallMs;
      baseMicroseconds = monotonic;
      candidate = wallMs;
   }
   if(candidate <= lastResult) candidate = lastResult + 1;
   lastResult = candidate;
   return candidate;
}

string AccountId()
{
   return LongText((long)AccountInfoInteger(ACCOUNT_LOGIN));
}

bool IsLoopbackHost(const string host)
{
   return host == "127.0.0.1" || host == "localhost" || host == "::1";
}

string AccountTradeModeText()
{
   ENUM_ACCOUNT_TRADE_MODE mode = (ENUM_ACCOUNT_TRADE_MODE)AccountInfoInteger(ACCOUNT_TRADE_MODE);
   if(mode == ACCOUNT_TRADE_MODE_REAL)    return "REAL";
   if(mode == ACCOUNT_TRADE_MODE_CONTEST) return "CONTEST";
   return "DEMO";
}

string AccountMarginModeText()
{
   ENUM_ACCOUNT_MARGIN_MODE mode = (ENUM_ACCOUNT_MARGIN_MODE)AccountInfoInteger(ACCOUNT_MARGIN_MODE);
   if(mode == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING) return "HEDGING";
   return "NETTING";
}

string UtcRiskDay()
{
   MqlDateTime part;
   TimeToStruct(TimeGMT(), part);
   return StringFormat("%04d-%02d-%02d", part.year, part.mon, part.day);
}

string SafeKeyPart(string value)
{
   StringReplace(value, "-", "_");
   StringReplace(value, ".", "_");
   StringReplace(value, " ", "_");
   return value;
}

string BaselineGlobalKey(const string day)
{
   return "QSX4_BASE_" + SafeKeyPart(AccountId()) + "_" + SafeKeyPart(day);
}

string HaltGlobalKey(const string day)
{
   return "QSX4_HALT_" + SafeKeyPart(AccountId()) + "_" + SafeKeyPart(day);
}

string BaselineVerifiedGlobalKey(const string day)
{
   return "QSX4_BASE_OK_" + SafeKeyPart(AccountId()) + "_" + SafeKeyPart(day);
}

string CursorGlobalKey()
{
   return "QSX4_CURSOR_" + SafeKeyPart(AccountId()) + "_" + IntegerToString(InpMagicNumber);
}

string CursorDealHighGlobalKey()
{
   return "QSX4_CURSOR_DEAL_HI_" + SafeKeyPart(AccountId()) + "_" + IntegerToString(InpMagicNumber);
}

string CursorDealLowGlobalKey()
{
   return "QSX4_CURSOR_DEAL_LO_" + SafeKeyPart(AccountId()) + "_" + IntegerToString(InpMagicNumber);
}

string CursorCommitGlobalKey()
{
   return "QSX4_CURSOR_COMMIT_" + SafeKeyPart(AccountId()) + "_" + IntegerToString(InpMagicNumber);
}

string PositionVersionGlobalKey(const string positionId)
{
   return "QSX4_PV_" + SafeKeyPart(AccountId()) + "_" +
          IntegerToString(InpMagicNumber) + "_" + SafeKeyPart(positionId);
}

string PositionVersionTimeGlobalKey(const string positionId)
{
   return "QSX4_PVT_" + SafeKeyPart(AccountId()) + "_" +
          IntegerToString(InpMagicNumber) + "_" + SafeKeyPart(positionId);
}

string PositionVersionCommitGlobalKey(const string positionId)
{
   return "QSX4_PVC_" + SafeKeyPart(AccountId()) + "_" +
          IntegerToString(InpMagicNumber) + "_" + SafeKeyPart(positionId);
}

string CacheFileName()
{
   return "QuantSwarmXAU_v4_commands_" + SafeKeyPart(AccountId()) + "_" +
          IntegerToString(InpMagicNumber) + ".log";
}

string NewEpoch()
{
   return AccountId() + "-" + LongText(UtcNowMs()) + "-" +
          IntegerToString((int)GetTickCount());
}

string NewMessageId(const string prefix)
{
   static long counter = 0;
   counter++;
   return prefix + "-" + g_ConnectionEpoch + "-" + LongText(counter);
}

string TradeComment(const string tradeId)
{
   string safe = tradeId;
   StringReplace(safe, "|", "-");
   return "QSX|" + StringSubstr(safe, 0, 24);
}

string TradeIdFromComment(const string comment)
{
   if(StringFind(comment, "QSX|") == 0)
      return StringSubstr(comment, 4);
   return "UNATTRIBUTED";
}

bool ParseUlongText(const string text, ulong &value)
{
   value = 0;
   int length = StringLen(text);
   if(length <= 0) return false;
   for(int i = 0; i < length; i++)
   {
      ushort c = StringGetCharacter(text, i);
      if(c < 48 || c > 57) return false;
      ulong digit = (ulong)(c - 48);
      if(value > (ULONG_MAX - digit) / 10) return false;
      value = value * 10 + digit;
   }
   return true;
}

//+------------------------------------------------------------------+
//| Flat compact JSON parser                                         |
//+------------------------------------------------------------------+
bool IsFlatJsonObject(const string &json)
{
   bool inString = false;
   bool escaped  = false;
   int depth = 0;
   int length = StringLen(json);
   for(int i = 0; i < length; i++)
   {
      ushort c = StringGetCharacter(json, i);
      if(inString)
      {
         if(escaped) { escaped = false; continue; }
         if(c == 92) { escaped = true; continue; }
         if(c == 34) inString = false;
         continue;
      }
      if(c == 34) { inString = true; continue; }
      if(c == 91 || c == 93) return false;
      if(c == 123)
      {
         depth++;
         if(depth > 1) return false;
      }
      else if(c == 125)
      {
         depth--;
         if(depth < 0) return false;
      }
   }
   return !inString && depth == 0;
}

int JsonValueStart(const string &json, const string key)
{
   string needle = "\"" + key + "\":";
   int at = StringFind(json, needle);
   if(at < 0) return -1;
   return at + StringLen(needle);
}

int JsonKeyCount(const string &json, const string key)
{
   string needle = "\"" + key + "\":";
   int count = 0;
   int offset = 0;
   while(true)
   {
      int at = StringFind(json, needle, offset);
      if(at < 0) break;
      count++;
      offset = at + StringLen(needle);
   }
   return count;
}

bool RequireUniqueField(const string &json, const string key, string &reason)
{
   int count = JsonKeyCount(json, key);
   if(count == 1) return true;
   reason = (count == 0 ? "MISSING_FIELD:" : "DUPLICATE_FIELD:") + key;
   return false;
}

string JsonGetString(const string &json, const string key)
{
   int start = JsonValueStart(json, key);
   if(start < 0 || StringGetCharacter(json, start) != 34) return "";
   string result = "";
   bool escaped = false;
   int length = StringLen(json);
   for(int i = start + 1; i < length; i++)
   {
      ushort c = StringGetCharacter(json, i);
      if(escaped)
      {
         if(c == 110) result += "\n";
         else if(c == 114) result += "\r";
         else if(c == 116) result += "\t";
         else result += StringSubstr(json, i, 1);
         escaped = false;
         continue;
      }
      if(c == 92) { escaped = true; continue; }
      if(c == 34) return result;
      result += StringSubstr(json, i, 1);
   }
   return "";
}

string JsonGetRaw(const string &json, const string key)
{
   int start = JsonValueStart(json, key);
   if(start < 0) return "";
   int length = StringLen(json);
   int finish = start;
   while(finish < length)
   {
      ushort c = StringGetCharacter(json, finish);
      if(c == 44 || c == 125) break;
      finish++;
   }
   return StringSubstr(json, start, finish - start);
}

double JsonGetDouble(const string &json, const string key)
{
   return StringToDouble(JsonGetRaw(json, key));
}

long JsonGetLong(const string &json, const string key)
{
   return StringToInteger(JsonGetRaw(json, key));
}

bool JsonGetBool(const string &json, const string key)
{
   return JsonGetRaw(json, key) == "true";
}

//+------------------------------------------------------------------+
//| Protocol envelope and reliable socket I/O                        |
//+------------------------------------------------------------------+
string EnvelopeStart(const string type, const string correlationId)
{
   g_SendSequence++;
   return "{\"v\":\"" + QSX_PROTOCOL_VERSION + "\",\"type\":" + J(type) +
          ",\"msg_id\":" + J(NewMessageId(type)) +
          ",\"auth\":" + J(InpAuthToken) +
          ",\"epoch\":" + J(g_ConnectionEpoch) +
          ",\"seq\":" + LongText(g_SendSequence) +
          ",\"correlation_id\":" + J(correlationId) +
          ",\"ts_ms\":" + LongText(UtcNowMs());
}

bool SendAll(const string frame)
{
   if(g_Socket == INVALID_HANDLE || !g_Connected) return false;
   uchar bytes[];
   int total = StringToCharArray(frame, bytes, 0, WHOLE_ARRAY, CP_UTF8) - 1;
   if(total <= 0 || total > InpMaxFrameBytes)
   {
      Print("[QSX4] Refusing invalid outbound frame length: ", total);
      return false;
   }
   int offset = 0;
   while(offset < total)
   {
      int remaining = total - offset;
      uchar chunk[];
      ArrayResize(chunk, remaining);
      ArrayCopy(chunk, bytes, 0, offset, remaining);
      int sent = SocketSend(g_Socket, chunk, remaining);
      if(sent <= 0)
      {
         DisconnectSocket();
         return false;
      }
      offset += sent;
   }
   return true;
}

bool SendMessage(const string type, const string correlationId, const string fields)
{
   string frame = EnvelopeStart(type, correlationId) + fields + "}\n";
   return SendAll(frame);
}

string ReadLine()
{
   if(g_Socket == INVALID_HANDLE || !g_Connected) return "";
   int newline = StringFind(g_ReceiveBuffer, "\n");
   if(newline >= 0)
   {
      if(newline > InpMaxFrameBytes)
      {
         Print("[QSX4] Oversized inbound frame; disconnecting");
         DisconnectSocket();
         return "";
      }
      string ready = StringSubstr(g_ReceiveBuffer, 0, newline);
      g_ReceiveBuffer = StringSubstr(g_ReceiveBuffer, newline + 1);
      return ready;
   }

   uint available = SocketIsReadable(g_Socket);
   if(available == 0) return "";
   if(available > (uint)InpMaxFrameBytes + 1) available = (uint)InpMaxFrameBytes + 1;
   uchar buffer[];
   ArrayResize(buffer, (int)available);
   int received = SocketRead(g_Socket, buffer, available, 100);
   if(received <= 0) return "";
   g_ReceiveBuffer += CharArrayToString(buffer, 0, received, CP_UTF8);
   newline = StringFind(g_ReceiveBuffer, "\n");
   if(newline >= 0)
   {
      if(newline > InpMaxFrameBytes)
      {
         Print("[QSX4] Oversized inbound frame; disconnecting");
         DisconnectSocket();
         return "";
      }
      string line = StringSubstr(g_ReceiveBuffer, 0, newline);
      g_ReceiveBuffer = StringSubstr(g_ReceiveBuffer, newline + 1);
      return line;
   }
   if(StringLen(g_ReceiveBuffer) > InpMaxFrameBytes)
   {
      Print("[QSX4] Oversized inbound frame; disconnecting");
      DisconnectSocket();
      return "";
   }
   return "";
}

bool ValidateIncomingEnvelope(const string &json, string &messageType, string &reason)
{
   if(StringLen(json) <= 0 || StringLen(json) > InpMaxFrameBytes)
   {
      reason = "FRAME_SIZE"; return false;
   }
   if(!IsFlatJsonObject(json))
   {
      reason = "NOT_FLAT_JSON"; return false;
   }
   string commonFields[] = {"v", "type", "msg_id", "auth", "epoch", "seq",
                            "correlation_id", "ts_ms"};
   for(int i = 0; i < ArraySize(commonFields); i++)
      if(!RequireUniqueField(json, commonFields[i], reason)) return false;
   if(JsonGetString(json, "v") != QSX_PROTOCOL_VERSION)
   {
      reason = "PROTOCOL_VERSION"; return false;
   }
   if(JsonGetString(json, "auth") != InpAuthToken)
   {
      reason = "AUTH_FAILED"; return false;
   }
   if(JsonGetString(json, "epoch") != g_ConnectionEpoch)
   {
      reason = "EPOCH_MISMATCH"; return false;
   }
   long sequence = JsonGetLong(json, "seq");
   if(sequence < 1 || sequence <= g_LastRecvSequence)
   {
      reason = "SEQUENCE_REPLAY"; return false;
   }
   messageType = JsonGetString(json, "type");
   if(messageType == "" || JsonGetString(json, "msg_id") == "" ||
      JsonGetLong(json, "ts_ms") < 1)
   {
      reason = "MISSING_TYPE"; return false;
   }
   if(!g_Authenticated && messageType != "HELLO_ACK")
   {
      reason = "SESSION_NOT_AUTHENTICATED"; return false;
   }
   g_LastRecvSequence = sequence;
   return true;
}

void DisconnectSocket()
{
   if(g_Socket != INVALID_HANDLE) SocketClose(g_Socket);
   g_Socket = INVALID_HANDLE;
   g_Connected = false;
   g_Authenticated = false;
   g_ReceiveBuffer = "";
}

void MarkLocalStateFailure(const string reason)
{
   g_LocalStateHealthy = false;
   g_DailyHalted = true;
   Print("[QSX4] LOCAL_STATE_FAILURE: ", reason,
         "; broker mutation and reconnect are disabled");
}

bool ReadVerifiedGlobal(const string key, double &value)
{
   value = 0.0;
   if(!GlobalVariableCheck(key) || !GlobalVariableGet(key, value)) return false;
   return MathIsValidNumber(value);
}

bool SetVerifiedGlobal(const string key, const double value)
{
   if(!MathIsValidNumber(value)) return false;
   ResetLastError();
   if(GlobalVariableSet(key, value) == 0) return false;
   GlobalVariablesFlush();
   double observed = 0.0;
   return ReadVerifiedGlobal(key, observed) && observed == value;
}

bool BaselineApprovalMatches(const string day, const double baseline)
{
   if(InpApproveBaselineUtcDay == "" ||
      InpApproveBaselineEquity <= 0.0)
      return false;

   if(InpApproveBaselineUtcDay != day)
      return false;

   // Approval never replaces the baseline. It must match the value that was
   // already persisted by the EA for this exact UTC risk day.
   return MathAbs(InpApproveBaselineEquity - baseline) <= 0.0000001;
}

double CursorCommitValue(const long timeMs, const ulong dealId)
{
   ulong timePart = (ulong)(timeMs % 1000003);
   ulong commit = timePart * 1000000007 + (dealId % 1000000007);
   return (double)commit;
}

double PositionVersionCommitValue(const long brokerTimeMs, const long version)
{
   ulong timePart = (ulong)(brokerTimeMs % 1000003);
   ulong versionPart = (ulong)(version % 1000000007);
   return (double)(timePart * 1000000007 + versionPart);
}

bool LoadDealCursorState()
{
   bool hasTime = GlobalVariableCheck(CursorGlobalKey());
   bool hasHigh = GlobalVariableCheck(CursorDealHighGlobalKey());
   bool hasLow = GlobalVariableCheck(CursorDealLowGlobalKey());
   bool hasCommit = GlobalVariableCheck(CursorCommitGlobalKey());
   if(!hasTime && !hasHigh && !hasLow && !hasCommit)
   {
      g_LastDealCursorMs = 0;
      g_LastDealCursorDeal = 0;
      return true;
   }
   if(!(hasTime && hasHigh && hasLow && hasCommit)) return false;

   double timeValue = 0.0, highValue = 0.0, lowValue = 0.0, commitValue = 0.0;
   if(!ReadVerifiedGlobal(CursorGlobalKey(), timeValue) ||
      !ReadVerifiedGlobal(CursorDealHighGlobalKey(), highValue) ||
      !ReadVerifiedGlobal(CursorDealLowGlobalKey(), lowValue) ||
      !ReadVerifiedGlobal(CursorCommitGlobalKey(), commitValue) ||
      timeValue < 0.0 || timeValue != MathFloor(timeValue) ||
      highValue < 0.0 || highValue > 4294967295.0 || highValue != MathFloor(highValue) ||
      lowValue < 0.0 || lowValue > 4294967295.0 || lowValue != MathFloor(lowValue))
      return false;

   long timeMs = (long)timeValue;
   ulong dealId = (((ulong)highValue) << 32) | (ulong)lowValue;
   if(commitValue != CursorCommitValue(timeMs, dealId)) return false;
   g_LastDealCursorMs = timeMs;
   g_LastDealCursorDeal = dealId;
   return true;
}

//+------------------------------------------------------------------+
//| Durable command idempotency                                      |
//+------------------------------------------------------------------+
string CleanCacheField(string value)
{
   StringReplace(value, "|", "/");
   StringReplace(value, "\r", " ");
   StringReplace(value, "\n", " ");
   return value;
}

int FindCommand(const string commandId)
{
   for(int i = ArraySize(g_CommandCache) - 1; i >= 0; i--)
      if(g_CommandCache[i].command_id == commandId) return i;
   return -1;
}

bool CommandProtectsOpenPosition(const CommandRecord &record)
{
   if(record.position_id != "" && record.position_id != "0" &&
      FindTicketByIdentifier(record.position_id) != 0) return true;
   ulong orderTicket = 0;
   return ParseUlongText(record.order_id, orderTicket) &&
          SelectOwnedPendingOrder(orderTicket);
}

void UpsertCommand(const CommandRecord &record)
{
   int index = FindCommand(record.command_id);
   if(index >= 0)
   {
      g_CommandCache[index] = record;
      return;
   }
   int size = ArraySize(g_CommandCache);
   while(size >= QSX_CACHE_LIMIT)
   {
      int evict = -1;
      for(int i = 0; i < size; i++)
      {
         if(!CommandProtectsOpenPosition(g_CommandCache[i])) { evict = i; break; }
      }
      if(evict >= 0)
      {
         for(int i = evict + 1; i < size; i++) g_CommandCache[i - 1] = g_CommandCache[i];
         size--;
         ArrayResize(g_CommandCache, size);
      }
      else break;
   }
   ArrayResize(g_CommandCache, size + 1);
   g_CommandCache[size] = record;
}

string CommandCacheLine(const CommandRecord &record)
{
   return CleanCacheField(record.command_id) + "|" +
          CleanCacheField(record.trade_id) + "|" +
          CleanCacheField(record.action) + "|" +
          CleanCacheField(record.direction) + "|" +
          CleanCacheField(record.status) + "|" +
          CleanCacheField(record.reason) + "|" +
          IntegerToString((int)record.retcode) + "|" +
          CleanCacheField(record.order_id) + "|" +
          CleanCacheField(record.deal_id) + "|" +
          CleanCacheField(record.position_id) + "|" +
          CleanCacheField(record.position_ticket) + "|" +
          DoubleToString(record.actual_price, _Digits) + "|" +
          DoubleToString(record.actual_volume, 8) + "|" +
          LongText(record.expected_position_version) + "|" +
          DoubleToString(record.target_sl, _Digits) + "|" +
          DoubleToString(record.final_tp, _Digits);
}

bool PersistCommand(const CommandRecord &record)
{
   string expected = CommandCacheLine(record);
   int file = FileOpen(CacheFileName(), FILE_COMMON | FILE_READ | FILE_WRITE | FILE_TXT | FILE_ANSI);
   if(file == INVALID_HANDLE) return false;
   if(!FileSeek(file, 0, SEEK_END) ||
      FileWriteString(file, expected + "\r\n") == 0)
   {
      FileClose(file);
      return false;
   }
   FileFlush(file);
   FileClose(file);

   file = FileOpen(CacheFileName(), FILE_COMMON | FILE_READ | FILE_TXT | FILE_ANSI);
   if(file == INVALID_HANDLE) return false;
   string last = "";
   while(!FileIsEnding(file))
   {
      string line = FileReadString(file);
      if(line != "") last = line;
   }
   FileClose(file);
   return last == expected;
}

bool VerifyCommandCacheFile(const string fileName)
{
   int file = FileOpen(fileName, FILE_COMMON | FILE_READ | FILE_TXT | FILE_ANSI);
   if(file == INVALID_HANDLE) return false;
   int index = 0;
   bool valid = true;
   while(!FileIsEnding(file))
   {
      string line = FileReadString(file);
      if(line == "") continue;
      if(index >= ArraySize(g_CommandCache) ||
         line != CommandCacheLine(g_CommandCache[index]))
      {
         valid = false;
         break;
      }
      index++;
   }
   FileClose(file);
   return valid && index == ArraySize(g_CommandCache);
}

bool CompactCommandCache()
{
   string temporary = CacheFileName() + ".tmp";
   int file = FileOpen(temporary, FILE_COMMON | FILE_WRITE | FILE_TXT | FILE_ANSI);
   if(file == INVALID_HANDLE) return true; // The verified append journal remains authoritative.
   bool complete = true;
   for(int i = 0; i < ArraySize(g_CommandCache); i++)
   {
      if(FileWriteString(file, CommandCacheLine(g_CommandCache[i]) + "\r\n") == 0)
      {
         complete = false;
         break;
      }
   }
   FileFlush(file);
   FileClose(file);
   if(!complete)
   {
      FileDelete(temporary, FILE_COMMON);
      return true;
   }
   if(!VerifyCommandCacheFile(temporary))
   {
      FileDelete(temporary, FILE_COMMON);
      return true;
   }
   if(!FileMove(temporary, FILE_COMMON, CacheFileName(), FILE_COMMON | FILE_REWRITE))
   {
      Print("[QSX4] Command cache compaction deferred: ", GetLastError());
      return true;
   }
   return VerifyCommandCacheFile(CacheFileName());
}

bool LoadCommandCache()
{
   ArrayResize(g_CommandCache, 0);
   int file = FileOpen(CacheFileName(), FILE_COMMON | FILE_READ | FILE_TXT | FILE_ANSI);
   if(file == INVALID_HANDLE)
      return !FileIsExist(CacheFileName(), FILE_COMMON);
   while(!FileIsEnding(file))
   {
      string line = FileReadString(file);
      if(line == "") continue;
      string parts[];
      int count = StringSplit(line, StringGetCharacter("|", 0), parts);
      if(count != 13 && count != 16)
      {
         FileClose(file);
         return false;
      }
      CommandRecord record;
      record.command_id = parts[0]; record.trade_id = parts[1];
      record.action = parts[2]; record.direction = parts[3];
      record.status = parts[4]; record.reason = parts[5];
      record.retcode = (uint)StringToInteger(parts[6]);
      record.order_id = parts[7]; record.deal_id = parts[8];
      record.position_id = parts[9]; record.position_ticket = parts[10];
      record.actual_price = StringToDouble(parts[11]);
      record.actual_volume = StringToDouble(parts[12]);
      record.expected_position_version = (count == 16) ? StringToInteger(parts[13]) : 0;
      record.target_sl = (count == 16) ? StringToDouble(parts[14]) : 0.0;
      record.final_tp = (count == 16) ? StringToDouble(parts[15]) : 0.0;
      UpsertCommand(record);
   }
   FileClose(file);
   return CompactCommandCache();
}

bool StoreCommand(const CommandRecord &record)
{
   if(!g_LocalStateHealthy || !PersistCommand(record))
   {
      MarkLocalStateFailure("COMMAND_JOURNAL_WRITE_OR_VERIFY_FAILED");
      return false;
   }
   UpsertCommand(record);
   g_CommandWritesSinceCompact++;
   if(g_CommandWritesSinceCompact >= QSX_CACHE_LIMIT)
   {
      if(!CompactCommandCache())
      {
         MarkLocalStateFailure("COMMAND_JOURNAL_COMPACTION_VERIFY_FAILED");
         return false;
      }
      g_CommandWritesSinceCompact = 0;
   }
   return true;
}

//+------------------------------------------------------------------+
//| ACK/NACK                                                         |
//+------------------------------------------------------------------+
void SendAck(const CommandRecord &record, const string correlationId)
{
   string acknowledgement = "APPLIED";
   if(record.status == "PARTIAL_ACK") acknowledgement = "PARTIALLY_APPLIED";
   else if(record.status == "ACCEPTED") acknowledgement = "ACCEPTED";
   string fields = ",\"command_id\":" + J(record.command_id) +
      ",\"trade_id\":" + J(record.trade_id) +
      ",\"action\":" + J(record.action) +
      ",\"status\":" + J(acknowledgement) +
      ",\"retcode\":" + IntegerToString((int)record.retcode) +
      ",\"retcode_desc\":" + J(record.reason) +
      ",\"order_id\":" + J(record.order_id) +
      ",\"deal_id\":" + J(record.deal_id) +
      ",\"position_id\":" + J(record.position_id) +
      ",\"position_ticket\":" + J(record.position_ticket) +
      ",\"actual_price\":" + DoubleToString(record.actual_price, _Digits) +
      ",\"actual_volume\":" + DoubleToString(record.actual_volume, 8);
   SendMessage("COMMAND_ACK", correlationId, fields);
}

void SendNackValues(const string commandId, const string tradeId,
                    const string action, const string reason,
                    const uint retcode, const string retcodeDescription,
                    const string correlationId)
{
   string fields = ",\"command_id\":" + J(commandId) +
      ",\"trade_id\":" + J(tradeId) +
      ",\"action\":" + J(action) +
      ",\"status\":\"REJECTED\"" +
      ",\"reason\":" + J(reason) +
      ",\"retcode\":" + IntegerToString((int)retcode) +
      ",\"retcode_desc\":" + J(retcodeDescription);
   SendMessage("COMMAND_NACK", correlationId, fields);
}

void RejectAndStore(const string commandId, const string tradeId,
                    const string action, const string direction,
                    const string reason, const uint retcode,
                    const string description, const string correlationId)
{
   CommandRecord record;
   record.command_id = commandId; record.trade_id = tradeId;
   record.action = action; record.direction = direction;
   record.status = "NACK"; record.reason = reason + ":" + description;
   record.retcode = retcode; record.order_id = "0"; record.deal_id = "0";
   record.position_id = "0"; record.position_ticket = "0";
   record.actual_price = 0.0; record.actual_volume = 0.0;
   record.expected_position_version = 0; record.target_sl = 0.0; record.final_tp = 0.0;
   if(!StoreCommand(record))
   {
      SendNackValues(commandId, tradeId, action, "LOCAL_COMMAND_PERSISTENCE_FAILED",
                     0, "rejected command could not be durably journaled", correlationId);
      DisconnectSocket();
      return;
   }
   SendNackValues(commandId, tradeId, action, reason, retcode, description, correlationId);
}

bool ReplayCommandIfKnown(const string commandId, const string tradeId,
                          const string action, const string correlationId)
{
   int index = FindCommand(commandId);
   if(index < 0) return false;
   CommandRecord record = g_CommandCache[index];
   if(record.trade_id != tradeId || record.action != action)
   {
      SendNackValues(commandId, tradeId, action, "COMMAND_ID_COLLISION", 0,
                     "command_id already belongs to another immutable command",
                     correlationId);
      return true;
   }
   if(record.status == "ACK" || record.status == "PARTIAL_ACK" || record.status == "ACCEPTED")
      SendAck(record, correlationId);
   else if(record.status == "PENDING")
   {
      // PENDING proves only that the intent was durable before the broker call.
      // A crash may have happened before or after the broker mutation, so a
      // terminal NACK would falsely prove non-execution and release risk.
      Print("[QSX4] Ambiguous PENDING replay; disconnecting for reconciliation: ",
            record.command_id);
      DisconnectSocket();
   }
   else
      SendNackValues(record.command_id, record.trade_id, record.action,
                     "IDEMPOTENT_REPLAY", record.retcode, record.reason, correlationId);
   return true;
}

//+------------------------------------------------------------------+
//| Broker metadata, position identity and risk day                  |
//+------------------------------------------------------------------+
int LotDigits(double step)
{
   for(int digits = 0; digits <= 8; digits++)
      if(MathAbs(step * MathPow(10.0, digits) - MathRound(step * MathPow(10.0, digits))) < 1e-8)
         return digits;
   return 2;
}

double NormalizeVolumeDown(const double requested)
{
   double minimum = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maximum = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(minimum <= 0 || maximum <= 0 || step <= 0) return 0.0;
   double value = MathFloor(requested / step + 1e-9) * step;
   value = MathMin(value, maximum);
   value = NormalizeDouble(value, LotDigits(step));
   if(value < minimum) return 0.0;
   return value;
}

bool PriceAligned(const double price)
{
   double tick = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tick <= 0) return false;
   return MathAbs(price / tick - MathRound(price / tick)) < 1e-7;
}

int CountOwnedPositions()
{
   int count = 0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) continue;
      if(PositionGetString(POSITION_SYMBOL) == _Symbol &&
         (int)PositionGetInteger(POSITION_MAGIC) == InpMagicNumber) count++;
   }
   return count;
}

int CountAccountPositions()
{
   int count = 0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket != 0 && PositionSelectByTicket(ticket)) count++;
   }
   return count;
}

bool IsPendingOrderType(const ENUM_ORDER_TYPE type)
{
   return type == ORDER_TYPE_BUY_LIMIT || type == ORDER_TYPE_SELL_LIMIT ||
          type == ORDER_TYPE_BUY_STOP || type == ORDER_TYPE_SELL_STOP ||
          type == ORDER_TYPE_BUY_STOP_LIMIT || type == ORDER_TYPE_SELL_STOP_LIMIT;
}

bool SelectOwnedPendingOrder(const ulong ticket)
{
   if(ticket == 0 || !OrderSelect(ticket)) return false;
   ENUM_ORDER_TYPE type = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
   return IsPendingOrderType(type) && OrderGetString(ORDER_SYMBOL) == _Symbol &&
          (int)OrderGetInteger(ORDER_MAGIC) == InpMagicNumber;
}

int CountOwnedPendingOrders()
{
   int count = 0;
   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(SelectOwnedPendingOrder(ticket)) count++;
   }
   return count;
}

int CountAccountPendingOrders()
{
   int count = 0;
   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket)) continue;
      if(IsPendingOrderType((ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE))) count++;
   }
   return count;
}

double OwnedExposureRisk(bool &valid)
{
   valid = true;
   double totalRisk = 0.0;
   MqlTick tick;
   if(!SymbolInfoTick(_Symbol, tick)) { valid = false; return 0.0; }
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(!SelectOwnedPosition(ticket)) continue;
      double sl = PositionGetDouble(POSITION_SL);
      if(sl <= 0) { valid = false; return 0.0; }
      ENUM_POSITION_TYPE positionType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      ENUM_ORDER_TYPE orderType = (positionType == POSITION_TYPE_BUY) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
      double currentPrice = (positionType == POSITION_TYPE_BUY) ? tick.bid : tick.ask;
      double outcome = 0.0;
      if(!OrderCalcProfit(orderType, _Symbol, PositionGetDouble(POSITION_VOLUME),
                          currentPrice, sl, outcome))
      { valid = false; return 0.0; }
      if(outcome < 0) totalRisk += -outcome;
   }
   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(!SelectOwnedPendingOrder(ticket)) continue;
      double sl = OrderGetDouble(ORDER_SL);
      if(sl <= 0) { valid = false; return 0.0; }
      ENUM_ORDER_TYPE pendingType = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
      ENUM_ORDER_TYPE side = (pendingType == ORDER_TYPE_BUY_LIMIT ||
                              pendingType == ORDER_TYPE_BUY_STOP ||
                              pendingType == ORDER_TYPE_BUY_STOP_LIMIT) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
      double outcome = 0.0;
      if(!OrderCalcProfit(side, _Symbol, OrderGetDouble(ORDER_VOLUME_CURRENT),
                          OrderGetDouble(ORDER_PRICE_OPEN), sl, outcome))
      { valid = false; return 0.0; }
      if(outcome < 0) totalRisk += -outcome;
   }
   return totalRisk;
}

string PendingOrderTypeText(const ENUM_ORDER_TYPE type)
{
   if(type == ORDER_TYPE_BUY_LIMIT) return "BUY_LIMIT";
   if(type == ORDER_TYPE_SELL_LIMIT) return "SELL_LIMIT";
   if(type == ORDER_TYPE_BUY_STOP) return "BUY_STOP";
   if(type == ORDER_TYPE_SELL_STOP) return "SELL_STOP";
   if(type == ORDER_TYPE_BUY_STOP_LIMIT) return "BUY_STOP_LIMIT";
   if(type == ORDER_TYPE_SELL_STOP_LIMIT) return "SELL_STOP_LIMIT";
   return "UNKNOWN";
}

string OrderStateText(const ENUM_ORDER_STATE state)
{
   if(state == ORDER_STATE_STARTED) return "STARTED";
   if(state == ORDER_STATE_PLACED) return "PLACED";
   if(state == ORDER_STATE_PARTIAL) return "PARTIAL";
   if(state == ORDER_STATE_FILLED) return "FILLED";
   if(state == ORDER_STATE_CANCELED) return "CANCELED";
   if(state == ORDER_STATE_REJECTED) return "REJECTED";
   if(state == ORDER_STATE_EXPIRED) return "EXPIRED";
   return "REQUEST_ADD";
}

bool SelectOwnedPosition(const ulong ticket)
{
   if(ticket == 0 || !PositionSelectByTicket(ticket)) return false;
   return PositionGetString(POSITION_SYMBOL) == _Symbol &&
          (int)PositionGetInteger(POSITION_MAGIC) == InpMagicNumber;
}

ulong FindTicketByIdentifier(const string positionId)
{
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol ||
         (int)PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;
      if(UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER)) == positionId) return ticket;
   }
   return 0;
}

ulong ResolveTicket(const string positionId, const string ticketText)
{
   ulong ticket = 0;
   if(!ParseUlongText(ticketText, ticket)) ticket = 0;
   if(SelectOwnedPosition(ticket))
   {
      if(positionId == "" || UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER)) == positionId)
         return ticket;
   }
   return FindTicketByIdentifier(positionId);
}

bool PersistPositionVersionState(const string positionId, const long brokerTimeMs,
                                 const long version)
{
   if(positionId == "" || positionId == "0" || brokerTimeMs < 1 || version < 1 ||
      version > 9007199254740991)
      return false;
   double commit = PositionVersionCommitValue(brokerTimeMs, version);
   if(!SetVerifiedGlobal(PositionVersionCommitGlobalKey(positionId), -1.0) ||
      !SetVerifiedGlobal(PositionVersionTimeGlobalKey(positionId), (double)brokerTimeMs) ||
      !SetVerifiedGlobal(PositionVersionGlobalKey(positionId), (double)version) ||
      !SetVerifiedGlobal(PositionVersionCommitGlobalKey(positionId), commit))
      return false;

   double storedTime = 0.0, storedVersion = 0.0, storedCommit = 0.0;
   return ReadVerifiedGlobal(PositionVersionTimeGlobalKey(positionId), storedTime) &&
          ReadVerifiedGlobal(PositionVersionGlobalKey(positionId), storedVersion) &&
          ReadVerifiedGlobal(PositionVersionCommitGlobalKey(positionId), storedCommit) &&
          storedTime == (double)brokerTimeMs && storedVersion == (double)version &&
          storedCommit == commit;
}

long PositionVersionForIdentity(const string positionId, const long brokerTimeMs,
                                const bool advance)
{
   if(positionId == "" || positionId == "0" || brokerTimeMs < 1 ||
      brokerTimeMs > 9007199254740)
   {
      MarkLocalStateFailure("INVALID_POSITION_VERSION_IDENTITY");
      return 0;
   }
   string timeKey = PositionVersionTimeGlobalKey(positionId);
   string versionKey = PositionVersionGlobalKey(positionId);
   string commitKey = PositionVersionCommitGlobalKey(positionId);
   bool hasTime = GlobalVariableCheck(timeKey);
   bool hasVersion = GlobalVariableCheck(versionKey);
   bool hasCommit = GlobalVariableCheck(commitKey);
   long version = brokerTimeMs * 1000;
   if(!hasTime && !hasVersion && !hasCommit)
   {
      if(!PersistPositionVersionState(positionId, brokerTimeMs, version))
      {
         MarkLocalStateFailure("POSITION_VERSION_INITIALIZE_FAILED");
         return 0;
      }
      return version;
   }
   if(!(hasTime && hasVersion && hasCommit))
   {
      MarkLocalStateFailure("POSITION_VERSION_TORN_STATE");
      return 0;
   }

   double storedTimeValue = 0.0, storedVersionValue = 0.0, storedCommit = 0.0;
   if(!ReadVerifiedGlobal(timeKey, storedTimeValue) ||
      !ReadVerifiedGlobal(versionKey, storedVersionValue) ||
      !ReadVerifiedGlobal(commitKey, storedCommit) ||
      storedTimeValue < 1.0 || storedTimeValue != MathFloor(storedTimeValue) ||
      storedVersionValue < 1.0 || storedVersionValue != MathFloor(storedVersionValue))
   {
      MarkLocalStateFailure("POSITION_VERSION_READ_FAILED");
      return 0;
   }
   long storedTime = (long)storedTimeValue;
   long storedVersion = (long)storedVersionValue;
   if(storedCommit != PositionVersionCommitValue(storedTime, storedVersion) ||
      (!advance && brokerTimeMs < storedTime))
   {
      MarkLocalStateFailure("POSITION_VERSION_COMMIT_OR_TIME_REGRESSION");
      return 0;
   }
   if(!advance && brokerTimeMs == storedTime) return storedVersion;

   long effectiveBrokerTime = (brokerTimeMs > storedTime) ? brokerTimeMs : storedTime;
   version = effectiveBrokerTime * 1000;
   if(version <= storedVersion) version = storedVersion + 1;
   if(!PersistPositionVersionState(positionId, effectiveBrokerTime, version))
   {
      MarkLocalStateFailure("POSITION_VERSION_ADVANCE_FAILED");
      return 0;
   }
   return version;
}

long CurrentPositionVersion()
{
   long brokerTimeMs = (long)PositionGetInteger(POSITION_TIME_UPDATE_MSC);
   string positionId = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
   bool owned = PositionGetString(POSITION_SYMBOL) == _Symbol &&
                (int)PositionGetInteger(POSITION_MAGIC) == InpMagicNumber;
   if(!owned)
      return (brokerTimeMs > 0 && brokerTimeMs <= 9007199254740) ? brokerTimeMs * 1000 : 0;
   return PositionVersionForIdentity(positionId, brokerTimeMs, false);
}

string TradeIdForPosition(const string positionId, const string comment)
{
   for(int i = ArraySize(g_CommandCache) - 1; i >= 0; i--)
      if(g_CommandCache[i].position_id == positionId && g_CommandCache[i].trade_id != "")
         return g_CommandCache[i].trade_id;
   return TradeIdFromComment(comment);
}

bool UpdateUtcRiskDay()
{
   string day = UtcRiskDay();
   if(day == g_RiskDay && g_DayStartEquity > 0.0) return g_LocalStateHealthy;
   bool continuousRollover = g_RiskDay != "" && day != g_RiskDay;
   string baseKey = BaselineGlobalKey(day);
   string haltKey = HaltGlobalKey(day);
   string verifiedKey = BaselineVerifiedGlobalKey(day);
   double baseline = 0.0;
   bool verified = false;
   if(GlobalVariableCheck(baseKey))
   {
      double verifiedValue = 0.0;
      if(!ReadVerifiedGlobal(baseKey, baseline) || baseline <= 0.0 ||
         (GlobalVariableCheck(verifiedKey) &&
          !ReadVerifiedGlobal(verifiedKey, verifiedValue)))
      {
         MarkLocalStateFailure("DAILY_BASELINE_READ_FAILED");
         return false;
      }
      verified = verifiedValue > 0.5;

      if(!verified && BaselineApprovalMatches(day, baseline))
      {
         if(!SetVerifiedGlobal(verifiedKey, 1.0))
         {
            MarkLocalStateFailure(
               "DAILY_BASELINE_APPROVAL_WRITE_OR_VERIFY_FAILED"
            );
            return false;
         }

         verified = true;

         Print(
            "[QSX4] Existing UTC daily baseline explicitly approved for ",
            day,
            " at ",
            DoubleToString(baseline, 2)
         );
      }
   }
   else
   {
      baseline = AccountInfoDouble(ACCOUNT_EQUITY);
      verified = continuousRollover;
      if(baseline <= 0.0 || !MathIsValidNumber(baseline) ||
         !SetVerifiedGlobal(verifiedKey, 0.0) ||
         !SetVerifiedGlobal(baseKey, baseline) ||
         !SetVerifiedGlobal(verifiedKey, verified ? 1.0 : 0.0))
      {
         MarkLocalStateFailure("DAILY_BASELINE_WRITE_OR_VERIFY_FAILED");
         return false;
      }
   }
   double haltValue = 0.0;
   if(GlobalVariableCheck(haltKey) && !ReadVerifiedGlobal(haltKey, haltValue))
   {
      MarkLocalStateFailure("DAILY_HALT_READ_FAILED");
      return false;
   }
   g_RiskDay = day;
   g_DayStartEquity = baseline;
   g_BaselineVerified = verified;
   g_DailyHalted = haltValue > 0.5;
   g_DailyPnL = AccountInfoDouble(ACCOUNT_EQUITY) - baseline;
   return true;
}

bool SetDailyHalt(const bool halted)
{
   if(g_RiskDay == "" ||
      !SetVerifiedGlobal(HaltGlobalKey(g_RiskDay), halted ? 1.0 : 0.0))
   {
      MarkLocalStateFailure("DAILY_HALT_WRITE_OR_VERIFY_FAILED");
      g_DailyHalted = true;
      return false;
   }
   g_DailyHalted = halted;
   return true;
}

//+------------------------------------------------------------------+
//| Snapshots, replay, heartbeat and market data                     |
//+------------------------------------------------------------------+
void SendHello()
{
   string fields = ",\"role\":\"MT5_EA\",\"ea_version\":\"4.00\"" +
      ",\"account_id\":" + J(AccountId()) +
      ",\"broker_server\":" + J(AccountInfoString(ACCOUNT_SERVER)) +
      ",\"account_trade_mode\":" + J(AccountTradeModeText()) +
      ",\"trade_allowed\":" + (AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) ? "true" : "false") +
      ",\"expert_allowed\":" + (AccountInfoInteger(ACCOUNT_TRADE_EXPERT) ? "true" : "false") +
      ",\"symbol\":" + J(_Symbol) +
      ",\"magic\":" + IntegerToString(InpMagicNumber) +
      ",\"config_hash\":" + J(InpConfigHash) +
      ",\"terminal_build\":" + IntegerToString((int)TerminalInfoInteger(TERMINAL_BUILD));
   SendMessage("HELLO", "", fields);
}

bool SendAccountSnapshot(const string correlationId)
{
   if(!UpdateUtcRiskDay()) return false;
   string fields = ",\"account_id\":" + J(AccountId()) +
      ",\"broker_server\":" + J(AccountInfoString(ACCOUNT_SERVER)) +
      ",\"account_trade_mode\":" + J(AccountTradeModeText()) +
      ",\"currency\":" + J(AccountInfoString(ACCOUNT_CURRENCY)) +
      ",\"account_mode\":" + J(AccountMarginModeText()) +
      ",\"trade_allowed\":" + (AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) ? "true" : "false") +
      ",\"expert_allowed\":" + (AccountInfoInteger(ACCOUNT_TRADE_EXPERT) ? "true" : "false") +
      ",\"balance\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) +
      ",\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) +
      ",\"free_margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) +
      ",\"margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN), 2) +
      ",\"day_start_equity\":" + DoubleToString(g_DayStartEquity, 2) +
      ",\"risk_day\":" + J(g_RiskDay) +
      ",\"baseline_verified\":" + (g_BaselineVerified ? "true" : "false") +
      ",\"daily_halted\":" + (g_DailyHalted ? "true" : "false") +
      ",\"server_time_ms\":" + LongText(((long)TimeCurrent()) * 1000);
   return SendMessage("ACCOUNT_SNAPSHOT", correlationId, fields);
}

string FillingModeText()
{
   long mode = SymbolInfoInteger(_Symbol, SYMBOL_FILLING_MODE);
   return LongText(mode);
}

bool SendSymbolSnapshot(const string correlationId)
{
   string fields = ",\"symbol\":" + J(_Symbol) +
      ",\"digits\":" + IntegerToString(_Digits) +
      ",\"tick_size\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE), _Digits) +
      ",\"tick_value_profit\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE_PROFIT), 8) +
      ",\"tick_value_loss\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE_LOSS), 8) +
      ",\"contract_size\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_TRADE_CONTRACT_SIZE), 4) +
      ",\"volume_min\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN), 8) +
      ",\"volume_step\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP), 8) +
      ",\"volume_max\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX), 8) +
      ",\"stops_level\":" + LongText(SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL)) +
      ",\"freeze_level\":" + LongText(SymbolInfoInteger(_Symbol, SYMBOL_TRADE_FREEZE_LEVEL)) +
      ",\"trade_mode\":" + J(LongText(SymbolInfoInteger(_Symbol, SYMBOL_TRADE_MODE))) +
      ",\"filling_mode\":" + J(FillingModeText());
   return SendMessage("SYMBOL_SNAPSHOT", correlationId, fields);
}

string CurrentDealCursor()
{
   return LongText(g_LastDealCursorMs) + ":" + UlongText(g_LastDealCursorDeal);
}

bool ParseDealCursor(const string text, long &timeMs, ulong &dealId)
{
   timeMs = 0;
   dealId = 0;
   if(text == "" || text == "0") return true;
   int separator = StringFind(text, ":");
   string timePart = (separator < 0) ? text : StringSubstr(text, 0, separator);
   string dealPart = (separator < 0) ? "0" : StringSubstr(text, separator + 1);
   if(timePart == "" || dealPart == "") return false;
   for(int i = 0; i < StringLen(timePart); i++)
   {
      ushort c = StringGetCharacter(timePart, i);
      if(c < 48 || c > 57) return false;
   }
   timeMs = StringToInteger(timePart);
   return timeMs >= 0 && ParseUlongText(dealPart, dealId);
}

bool CursorAfter(const long timeMs, const ulong dealId,
                 const long cursorTimeMs, const ulong cursorDealId)
{
   return timeMs > cursorTimeMs || (timeMs == cursorTimeMs && dealId > cursorDealId);
}

string CursorText(const long timeMs, const ulong dealId)
{
   return LongText(timeMs) + ":" + UlongText(dealId);
}

bool AdvanceLocalDealCursor(const long timeMs, const ulong dealId)
{
   if(!CursorAfter(timeMs, dealId, g_LastDealCursorMs, g_LastDealCursorDeal)) return true;
   double high = (double)(dealId >> 32);
   double low = (double)(dealId & 0xFFFFFFFF);
   double commit = CursorCommitValue(timeMs, dealId);
   if(timeMs < 0 || !SetVerifiedGlobal(CursorCommitGlobalKey(), -1.0) ||
      !SetVerifiedGlobal(CursorGlobalKey(), (double)timeMs) ||
      !SetVerifiedGlobal(CursorDealHighGlobalKey(), high) ||
      !SetVerifiedGlobal(CursorDealLowGlobalKey(), low) ||
      !SetVerifiedGlobal(CursorCommitGlobalKey(), commit))
   {
      MarkLocalStateFailure("DEAL_CURSOR_WRITE_OR_VERIFY_FAILED");
      return false;
   }
   double observedTime = 0.0, observedHigh = 0.0, observedLow = 0.0;
   double observedCommit = 0.0;
   if(!ReadVerifiedGlobal(CursorGlobalKey(), observedTime) ||
      !ReadVerifiedGlobal(CursorDealHighGlobalKey(), observedHigh) ||
      !ReadVerifiedGlobal(CursorDealLowGlobalKey(), observedLow) ||
      !ReadVerifiedGlobal(CursorCommitGlobalKey(), observedCommit) ||
      observedTime != (double)timeMs || observedHigh != high || observedLow != low ||
      observedCommit != commit)
   {
      MarkLocalStateFailure("DEAL_CURSOR_COMMIT_MISMATCH");
      return false;
   }
   g_LastDealCursorMs = timeMs;
   g_LastDealCursorDeal = dealId;
   return true;
}

bool SendPositionSnapshot(const string correlationId)
{
   string snapshotId = NewMessageId("PS");
   // Reconciliation must see the complete account exposure.  Command handlers
   // remain symbol/magic fenced and may never mutate these foreign records.
   int count = CountAccountPositions();
   string begin = ",\"snapshot_id\":" + J(snapshotId) +
                  ",\"count\":" + IntegerToString(count) +
                  ",\"deal_cursor\":" + J(CurrentDealCursor());
   if(!SendMessage("POSITION_SNAPSHOT_BEGIN", correlationId, begin)) return false;

   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) continue;
      string positionSymbol = PositionGetString(POSITION_SYMBOL);
      int positionMagic = (int)PositionGetInteger(POSITION_MAGIC);
      int positionDigits = (int)SymbolInfoInteger(positionSymbol, SYMBOL_DIGITS);
      if(positionDigits < 0) positionDigits = _Digits;
      bool owned = positionSymbol == _Symbol && positionMagic == InpMagicNumber;
      string positionId = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
      string comment = PositionGetString(POSITION_COMMENT);
      string direction = ((ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY) ? "BUY" : "SELL";
      string tradeId = owned ? TradeIdForPosition(positionId, comment) :
                       "UNATTRIBUTED-" + positionId;
      long positionVersion = CurrentPositionVersion();
      if(positionVersion < 1) return false;
      string fields = ",\"snapshot_id\":" + J(snapshotId) +
         ",\"position_id\":" + J(positionId) +
         ",\"position_ticket\":" + J(UlongText(ticket)) +
         ",\"trade_id\":" + J(tradeId) +
         ",\"symbol\":" + J(positionSymbol) +
         ",\"magic\":" + IntegerToString(positionMagic) +
         ",\"direction\":" + J(direction) +
         ",\"volume\":" + DoubleToString(PositionGetDouble(POSITION_VOLUME), 8) +
         ",\"price_open\":" + DoubleToString(PositionGetDouble(POSITION_PRICE_OPEN), positionDigits) +
         ",\"sl\":" + DoubleToString(PositionGetDouble(POSITION_SL), positionDigits) +
         ",\"tp\":" + DoubleToString(PositionGetDouble(POSITION_TP), positionDigits) +
         ",\"position_version\":" + LongText(positionVersion) +
         ",\"open_time_ms\":" + LongText((long)PositionGetInteger(POSITION_TIME_MSC)) +
         ",\"comment\":" + J(comment);
      if(!SendMessage("POSITION_SNAPSHOT_ITEM", correlationId, fields)) return false;
   }
   string endFields = ",\"snapshot_id\":" + J(snapshotId) +
                      ",\"count\":" + IntegerToString(count) +
                      ",\"deal_cursor\":" + J(CurrentDealCursor());
   return SendMessage("POSITION_SNAPSHOT_END", correlationId, endFields);
}

bool SendPendingOrderSnapshot(const string correlationId)
{
   string snapshotId = NewMessageId("OS");
   int count = CountAccountPendingOrders();
   string begin = ",\"snapshot_id\":" + J(snapshotId) +
                  ",\"count\":" + IntegerToString(count);
   if(!SendMessage("PENDING_ORDER_SNAPSHOT_BEGIN", correlationId, begin)) return false;

   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket)) continue;
      ENUM_ORDER_TYPE orderType = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
      if(!IsPendingOrderType(orderType)) continue;
      string orderId = UlongText(ticket);
      string orderSymbol = OrderGetString(ORDER_SYMBOL);
      int orderMagic = (int)OrderGetInteger(ORDER_MAGIC);
      int orderDigits = (int)SymbolInfoInteger(orderSymbol, SYMBOL_DIGITS);
      if(orderDigits < 0) orderDigits = _Digits;
      bool owned = orderSymbol == _Symbol && orderMagic == InpMagicNumber;
      string comment = OrderGetString(ORDER_COMMENT);
      string tradeId = "UNATTRIBUTED-" + orderId;
      string commandId = "BROKER";
      if(owned)
      {
         CommandRecord context;
         ContextForIdentity(orderId, "0", "0", context);
         tradeId = context.trade_id;
         commandId = context.command_id;
         if(tradeId == "" || StringFind(tradeId, "UNATTRIBUTED-") == 0)
            tradeId = TradeIdFromComment(comment);
      }
      string fields = ",\"snapshot_id\":" + J(snapshotId) +
         ",\"order_id\":" + J(orderId) +
         ",\"trade_id\":" + J(tradeId) +
         ",\"command_id\":" + J(commandId) +
         ",\"symbol\":" + J(orderSymbol) +
         ",\"magic\":" + IntegerToString(orderMagic) +
         ",\"order_type\":" + J(PendingOrderTypeText(orderType)) +
         ",\"order_state\":" + J(OrderStateText((ENUM_ORDER_STATE)OrderGetInteger(ORDER_STATE))) +
         ",\"volume_initial\":" + DoubleToString(OrderGetDouble(ORDER_VOLUME_INITIAL), 8) +
         ",\"volume_current\":" + DoubleToString(OrderGetDouble(ORDER_VOLUME_CURRENT), 8) +
         ",\"price_open\":" + DoubleToString(OrderGetDouble(ORDER_PRICE_OPEN), orderDigits) +
         ",\"sl\":" + DoubleToString(OrderGetDouble(ORDER_SL), orderDigits) +
         ",\"tp\":" + DoubleToString(OrderGetDouble(ORDER_TP), orderDigits) +
         ",\"order_time_ms\":" + LongText((long)OrderGetInteger(ORDER_TIME_SETUP_MSC)) +
         ",\"expiration_time_ms\":" + LongText((long)OrderGetInteger(ORDER_TIME_EXPIRATION) * 1000) +
         ",\"comment\":" + J(comment);
      if(!SendMessage("PENDING_ORDER_SNAPSHOT_ITEM", correlationId, fields)) return false;
   }
   string endFields = ",\"snapshot_id\":" + J(snapshotId) +
                      ",\"count\":" + IntegerToString(count);
   return SendMessage("PENDING_ORDER_SNAPSHOT_END", correlationId, endFields);
}

bool DealIsOwned(const ulong deal)
{
   if(deal == 0 || !HistoryDealSelect(deal)) return false;
   return HistoryDealGetString(deal, DEAL_SYMBOL) == _Symbol &&
          (int)HistoryDealGetInteger(deal, DEAL_MAGIC) == InpMagicNumber;
}

string DealDirection(const ulong deal)
{
   ENUM_DEAL_TYPE type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(deal, DEAL_TYPE);
   ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(deal, DEAL_ENTRY);
   bool buySide = type == DEAL_TYPE_BUY;
   if(entry == DEAL_ENTRY_OUT || entry == DEAL_ENTRY_OUT_BY) buySide = !buySide;
   return buySide ? "BUY" : "SELL";
}

string DealEntryText(const ulong deal)
{
   ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(deal, DEAL_ENTRY);
   if(entry == DEAL_ENTRY_IN) return "IN";
   if(entry == DEAL_ENTRY_OUT) return "OUT";
   if(entry == DEAL_ENTRY_OUT_BY) return "OUT_BY";
   return "INOUT";
}

string DealReasonText(const ulong deal)
{
   ENUM_DEAL_REASON reason = (ENUM_DEAL_REASON)HistoryDealGetInteger(deal, DEAL_REASON);
   if(reason == DEAL_REASON_SL) return "SL";
   if(reason == DEAL_REASON_TP) return "TP";
   if(reason == DEAL_REASON_EXPERT) return "EXPERT";
   if(reason == DEAL_REASON_CLIENT) return "CLIENT";
   if(reason == DEAL_REASON_MOBILE) return "MOBILE";
   if(reason == DEAL_REASON_WEB) return "WEB";
   return IntegerToString((int)reason);
}

bool CachedModifyContextForPosition(const string positionId, CommandRecord &record)
{
   if(positionId == "" || positionId == "0") return false;
   ulong ticket = FindTicketByIdentifier(positionId);
   if(ticket == 0 || !SelectOwnedPosition(ticket)) return false;
   long currentVersion = CurrentPositionVersion();
   double currentSl = PositionGetDouble(POSITION_SL);
   double currentTp = PositionGetDouble(POSITION_TP);
   double tolerance = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE) / 2.0;
   int match = -1;
   for(int i = ArraySize(g_CommandCache) - 1; i >= 0; i--)
   {
      CommandRecord candidate = g_CommandCache[i];
      bool terminalOrPending = candidate.status == "PENDING" ||
                               candidate.status == "ACCEPTED" ||
                               candidate.status == "ACK" ||
                               candidate.status == "PARTIAL_ACK";
      if(candidate.action != "MODIFY_SL" || candidate.position_id != positionId ||
         !terminalOrPending || candidate.expected_position_version < 1 ||
         candidate.target_sl <= 0.0 || candidate.final_tp <= 0.0 ||
         currentVersion <= candidate.expected_position_version ||
         MathAbs(currentSl - candidate.target_sl) > tolerance ||
         MathAbs(currentTp - candidate.final_tp) > tolerance)
         continue;
      if(match >= 0) return false;
      match = i;
   }
   if(match < 0) return false;
   record = g_CommandCache[match];
   return true;
}

void ContextForIdentity(const string orderId, const string dealId,
                        const string positionId, CommandRecord &record)
{
   if(g_HasActiveCommand)
   {
      bool compatible =
         (orderId != "0" && g_ActiveCommand.order_id == orderId) ||
         (dealId != "0" && g_ActiveCommand.deal_id == dealId) ||
         (positionId != "0" &&
          (g_ActiveCommand.position_id == positionId ||
           (g_ActiveCommand.action == "OPEN" &&
            (g_ActiveCommand.position_id == "" || g_ActiveCommand.position_id == "0"))));
      if(compatible)
      {
         record = g_ActiveCommand;
         return;
      }
   }
   for(int i = ArraySize(g_CommandCache) - 1; i >= 0; i--)
   {
      if((orderId != "0" && g_CommandCache[i].order_id == orderId) ||
         (dealId != "0" && g_CommandCache[i].deal_id == dealId))
      {
         record = g_CommandCache[i];
         return;
      }
   }
   if(orderId == "0" && dealId == "0" &&
      CachedModifyContextForPosition(positionId, record)) return;
   record.command_id = "BROKER";
   record.trade_id = "UNATTRIBUTED-" + positionId;
   record.action = "BROKER";
   record.direction = "UNKNOWN";
   record.status = "ACK";
}

double RemainingVolumeByIdentifier(const string positionId, string &ticketText,
                                   double &sl, double &tp, long &version,
                                   string &direction)
{
   ulong ticket = FindTicketByIdentifier(positionId);
   if(ticket == 0 || !SelectOwnedPosition(ticket))
   {
      ticketText = "0"; sl = 0; tp = 0; version = UtcNowMs();
      return 0.0;
   }
   ticketText = UlongText(ticket);
   sl = PositionGetDouble(POSITION_SL);
   tp = PositionGetDouble(POSITION_TP);
   // The deal handler advances the durable logical version with DEAL_TIME_MSC.
   // Do not initialize it here from a possibly later aggregate position state.
   version = (long)PositionGetInteger(POSITION_TIME_UPDATE_MSC);
   direction = ((ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY) ? "BUY" : "SELL";
   return PositionGetDouble(POSITION_VOLUME);
}

bool SendDealReplay(const string correlationId, const string cursorText)
{
   long cursorTime = 0;
   ulong cursorDeal = 0;
   if(!ParseDealCursor(cursorText, cursorTime, cursorDeal))
   {
      Print("[QSX4] Invalid peer deal cursor; refusing ambiguous replay: ", cursorText);
      return false;
   }
   datetime from = (cursorTime > 0) ? (datetime)(cursorTime / 1000) :
                   (TimeCurrent() - (datetime)((long)InpDealReplayLookbackDays * 86400));
   if(!HistorySelect(from, TimeCurrent()))
   {
      Print("[QSX4] HistorySelect failed for deal replay: ", GetLastError());
      return false;
   }
   int total = HistoryDealsTotal();
   int count = 0;
   ulong replayDeals[];
   long replayTimes[];
   for(int i = 0; i < total; i++)
   {
      ulong deal = HistoryDealGetTicket(i);
      if(!DealIsOwned(deal)) continue;
      long dealTime = (long)HistoryDealGetInteger(deal, DEAL_TIME_MSC);
      if(!CursorAfter(dealTime, deal, cursorTime, cursorDeal)) continue;
      ArrayResize(replayDeals, count + 1);
      ArrayResize(replayTimes, count + 1);
      replayDeals[count] = deal;
      replayTimes[count] = dealTime;
      count++;
   }
   for(int i = 1; i < count; i++)
   {
      ulong keyDeal = replayDeals[i];
      long keyTime = replayTimes[i];
      int j = i - 1;
      while(j >= 0 && CursorAfter(replayTimes[j], replayDeals[j], keyTime, keyDeal))
      {
         replayDeals[j + 1] = replayDeals[j];
         replayTimes[j + 1] = replayTimes[j];
         j--;
      }
      replayDeals[j + 1] = keyDeal;
      replayTimes[j + 1] = keyTime;
   }
   string replayId = NewMessageId("DR");
   if(!SendMessage("DEAL_REPLAY_BEGIN", correlationId,
      ",\"replay_id\":" + J(replayId) + ",\"cursor_from\":" + J(CursorText(cursorTime, cursorDeal)) +
      ",\"count\":" + IntegerToString(count))) return false;

   long nextCursorTime = cursorTime;
   ulong nextCursorDeal = cursorDeal;
   long replayVersionTime = -1;
   int replayVersionOrdinal = 0;
   for(int i = 0; i < count; i++)
   {
      ulong deal = replayDeals[i];
      long dealTime = replayTimes[i];
      if(!HistoryDealSelect(deal)) return false;
      string dealId = UlongText(deal);
      string orderId = UlongText((ulong)HistoryDealGetInteger(deal, DEAL_ORDER));
      string positionId = UlongText((ulong)HistoryDealGetInteger(deal, DEAL_POSITION_ID));
      string ticketText = "0";
      ulong currentTicket = FindTicketByIdentifier(positionId);
      if(currentTicket != 0) ticketText = UlongText(currentTicket);
      if(dealTime != replayVersionTime)
      {
         replayVersionTime = dealTime;
         replayVersionOrdinal = 0;
      }
      else
      {
         replayVersionOrdinal++;
         if(replayVersionOrdinal >= 1000) return false;
      }
      long version = dealTime * 1000 + replayVersionOrdinal;
      string direction = DealDirection(deal);
      CommandRecord context;
      ContextForIdentity(orderId, dealId, positionId, context);
      string comment = HistoryDealGetString(deal, DEAL_COMMENT);
      string fields = ",\"replay_id\":" + J(replayId) +
         ",\"cursor\":" + J(CursorText(dealTime, deal)) +
         ",\"deal_id\":" + J(dealId) + ",\"order_id\":" + J(orderId) +
         ",\"position_id\":" + J(positionId) +
         ",\"position_ticket\":" + J(ticketText) +
         ",\"trade_id\":" + J(context.trade_id) +
         ",\"command_id\":" + J(context.command_id) +
         ",\"symbol\":" + J(_Symbol) + ",\"magic\":" + IntegerToString(InpMagicNumber) +
         ",\"direction\":" + J(direction) +
         ",\"entry_type\":" + J(DealEntryText(deal)) +
         ",\"price\":" + DoubleToString(HistoryDealGetDouble(deal, DEAL_PRICE), _Digits) +
         ",\"volume\":" + DoubleToString(HistoryDealGetDouble(deal, DEAL_VOLUME), 8) +
         ",\"remaining_volume\":0.0" +
         ",\"remaining_volume_known\":false" +
         ",\"profit\":" + DoubleToString(HistoryDealGetDouble(deal, DEAL_PROFIT), 2) +
         ",\"commission\":" + DoubleToString(HistoryDealGetDouble(deal, DEAL_COMMISSION), 2) +
         ",\"swap\":" + DoubleToString(HistoryDealGetDouble(deal, DEAL_SWAP), 2) +
         ",\"fee\":" + DoubleToString(HistoryDealGetDouble(deal, DEAL_FEE), 2) +
         ",\"reason\":" + J(DealReasonText(deal)) +
         ",\"broker_time_ms\":" + LongText(dealTime) +
         ",\"position_version\":" + LongText(version) +
         ",\"comment\":" + J(comment);
      if(!SendMessage("DEAL_REPLAY_ITEM", correlationId, fields)) return false;
      if(CursorAfter(dealTime, deal, nextCursorTime, nextCursorDeal))
      {
         nextCursorTime = dealTime;
         nextCursorDeal = deal;
      }
   }
   if(!AdvanceLocalDealCursor(nextCursorTime, nextCursorDeal))
   {
      DisconnectSocket();
      return false;
   }
   return SendMessage("DEAL_REPLAY_END", correlationId,
      ",\"replay_id\":" + J(replayId) + ",\"cursor_next\":" + J(CursorText(nextCursorTime, nextCursorDeal)) +
      ",\"count\":" + IntegerToString(count));
}

bool SendFullSnapshot(const string correlationId, const string dealCursor)
{
   if(!SendAccountSnapshot(correlationId)) return false;
   if(!SendSymbolSnapshot(correlationId)) return false;
   if(!SendPositionSnapshot(correlationId)) return false;
   if(!SendPendingOrderSnapshot(correlationId)) return false;
   return SendDealReplay(correlationId, dealCursor);
}

void SendHeartbeat()
{
   if(!UpdateUtcRiskDay())
   {
      DisconnectSocket();
      return;
   }
   string fields = ",\"account_id\":" + J(AccountId()) +
      ",\"symbol\":" + J(_Symbol) +
      ",\"balance\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) +
      ",\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) +
      ",\"free_margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) +
      ",\"daily_pnl\":" + DoubleToString(g_DailyPnL, 2) +
      ",\"risk_day\":" + J(g_RiskDay) +
      ",\"baseline_verified\":" + (g_BaselineVerified ? "true" : "false") +
      ",\"daily_halted\":" + (g_DailyHalted ? "true" : "false") +
      ",\"owned_positions\":" + IntegerToString(CountOwnedPositions()) +
      ",\"last_deal_cursor\":" + J(CurrentDealCursor()) +
      ",\"owned_pending_orders\":" + IntegerToString(CountOwnedPendingOrders()) +
      ",\"server_time_ms\":" + LongText(((long)TimeCurrent()) * 1000);
   SendMessage("HEARTBEAT", "", fields);
}

bool SendBarRecord(const string label, const MqlRates &rate)
{
   string fields = ",\"symbol\":" + J(_Symbol) + ",\"tf\":" + J(label) +
      ",\"o\":" + DoubleToString(rate.open, _Digits) +
      ",\"h\":" + DoubleToString(rate.high, _Digits) +
      ",\"l\":" + DoubleToString(rate.low, _Digits) +
      ",\"c\":" + DoubleToString(rate.close, _Digits) +
      ",\"volume\":" + LongText((long)rate.tick_volume) +
      ",\"bar_time_ms\":" + LongText(((long)rate.time) * 1000);
   return SendMessage("BAR", "", fields);
}

bool SendHistoryTf(const string label, const ENUM_TIMEFRAMES timeframe,
                   const int requested, datetime &lastSent)
{
   if(!g_Authenticated || requested <= 0) return false;
   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   int copied = CopyRates(_Symbol, timeframe, 1, requested, rates);
   if(copied <= 0) return false;
   for(int i = 0; i < copied; i++)
   {
      if(!SendBarRecord(label, rates[i])) return false;
      lastSent = rates[i].time;
   }
   return true;
}

void SendNewCompletedBarsTf(const string label,
                            const ENUM_TIMEFRAMES timeframe,
                            const int maxBatch,
                            datetime &lastSent)
{
   if(!g_Authenticated) return;
   datetime newestCompleted = iTime(_Symbol, timeframe, 1);
   if(newestCompleted <= 0 || newestCompleted <= lastSent) return;

   int startPosition = 1;
   int count = MathMax(1, maxBatch);
   if(lastSent > 0)
   {
      int lastShift = iBarShift(_Symbol, timeframe, lastSent, true);
      if(lastShift < 0)
      {
         Print("[QSX4] Completed-bar cursor is no longer present for ", label,
               "; reconnecting for an explicit epoch reset");
         DisconnectSocket();
         return;
      }
      int pending = lastShift - 1;
      if(pending <= 0) return;
      count = MathMin(pending, count);
      // Copy the oldest pending chunk first so a long terminal stall cannot
      // silently skip bars when the catch-up set exceeds one bounded batch.
      startPosition = pending - count + 1;
   }

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   int copied = CopyRates(_Symbol, timeframe, startPosition, count, rates);
   if(copied <= 0) return;
   for(int i = 0; i < copied; i++)
   {
      if(rates[i].time <= lastSent) continue;
      if(!SendBarRecord(label, rates[i])) return;
      lastSent = rates[i].time;
   }
}

void SeedBarPublicationState()
{
   g_LastSentM1Bar = iTime(_Symbol, PERIOD_M1, 1);
   g_LastSentM5Bar = iTime(_Symbol, PERIOD_M5, 1);
   g_LastSentM15Bar = iTime(_Symbol, PERIOD_M15, 1);
   g_LastSentH1Bar = iTime(_Symbol, PERIOD_H1, 1);
   g_LastSentH4Bar = iTime(_Symbol, PERIOD_H4, 1);
   g_LastSentD1Bar = iTime(_Symbol, PERIOD_D1, 1);
}

void ResetBarPublicationState()
{
   g_LastSentM1Bar = 0;
   g_LastSentM5Bar = 0;
   g_LastSentM15Bar = 0;
   g_LastSentH1Bar = 0;
   g_LastSentH4Bar = 0;
   g_LastSentD1Bar = 0;
}

void SendHistory()
{
   SendHistoryTf("M1", PERIOD_M1, InpHistoryM1Bars, g_LastSentM1Bar);
   SendHistoryTf("M5", PERIOD_M5, InpHistoryM5Bars, g_LastSentM5Bar);
   SendHistoryTf("M15", PERIOD_M15, InpHistoryM15Bars, g_LastSentM15Bar);
   SendHistoryTf("H1", PERIOD_H1, InpHistoryH1Bars, g_LastSentH1Bar);
   SendHistoryTf("H4", PERIOD_H4, InpHistoryH4Bars, g_LastSentH4Bar);
   SendHistoryTf("D1", PERIOD_D1, InpHistoryD1Bars, g_LastSentD1Bar);
}

void SendCompletedBarUpdates()
{
   SendNewCompletedBarsTf("M1", PERIOD_M1, InpHistoryM1Bars, g_LastSentM1Bar);
   SendNewCompletedBarsTf("M5", PERIOD_M5, InpHistoryM5Bars, g_LastSentM5Bar);
   SendNewCompletedBarsTf("M15", PERIOD_M15, InpHistoryM15Bars, g_LastSentM15Bar);
   SendNewCompletedBarsTf("H1", PERIOD_H1, InpHistoryH1Bars, g_LastSentH1Bar);
   SendNewCompletedBarsTf("H4", PERIOD_H4, InpHistoryH4Bars, g_LastSentH4Bar);
   SendNewCompletedBarsTf("D1", PERIOD_D1, InpHistoryD1Bars, g_LastSentD1Bar);
}

//+------------------------------------------------------------------+
//| Command validation and execution                                 |
//+------------------------------------------------------------------+
bool IsRetriableRetcode(const uint retcode)
{
   return retcode == TRADE_RETCODE_REQUOTE ||
          retcode == TRADE_RETCODE_PRICE_CHANGED ||
          retcode == TRADE_RETCODE_PRICE_OFF;
}

bool IsSuccessRetcode(const uint retcode)
{
   return retcode == TRADE_RETCODE_DONE || retcode == TRADE_RETCODE_DONE_PARTIAL ||
          retcode == TRADE_RETCODE_PLACED;
}

bool TradingIdentityReady(string &reason)
{
   if(!g_LocalStateHealthy) { reason = "LOCAL_STATE_UNHEALTHY"; return false; }
   if(!g_Authenticated) { reason = "SESSION_NOT_AUTHENTICATED"; return false; }
   if(AccountId() == "0") { reason = "INVALID_ACCOUNT"; return false; }
   if(!AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) || !AccountInfoInteger(ACCOUNT_TRADE_EXPERT) ||
      !TerminalInfoInteger(TERMINAL_TRADE_ALLOWED))
   {
      reason = "TRADING_NOT_ALLOWED"; return false;
   }
   if(AccountTradeModeText() == "REAL" && !InpPermitRealAccount)
   {
      reason = "REAL_ACCOUNT_LOCAL_BLOCK"; return false;
   }
   return true;
}

bool ValidateOpenGeometry(const string direction, const double price, const double sl,
                          const double tp1, const double tp2, const double tp3,
                          string &reason)
{
   if(!PriceAligned(sl) || !PriceAligned(tp1) || !PriceAligned(tp2) || !PriceAligned(tp3))
   { reason = "PRICE_NOT_TICK_ALIGNED"; return false; }
   if(direction == "BUY")
   {
      if(!(sl > 0 && sl < price && price < tp1 && tp1 < tp2 && tp2 < tp3))
      { reason = "INVALID_BUY_GEOMETRY"; return false; }
   }
   else if(direction == "SELL")
   {
      if(!(tp3 > 0 && tp3 < tp2 && tp2 < tp1 && tp1 < price && price < sl))
      { reason = "INVALID_SELL_GEOMETRY"; return false; }
   }
   else { reason = "INVALID_DIRECTION"; return false; }

   double minimum = (double)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL) * _Point;
   double stopDistance = (direction == "BUY") ? price - sl : sl - price;
   double firstTargetDistance = (direction == "BUY") ? tp1 - price : price - tp1;
   if(stopDistance < minimum || firstTargetDistance < minimum)
   { reason = "BROKER_STOPS_LEVEL"; return false; }
   return true;
}

bool ValidatePositionVersion(const long expected, string &reason)
{
   if(expected < 1)
   {
      reason = "MISSING_POSITION_VERSION";
      return false;
   }
   if(CurrentPositionVersion() != expected)
   {
      reason = "POSITION_VERSION_MISMATCH";
      return false;
   }
   return true;
}

bool BeginActiveCommand(CommandRecord &record)
{
   record.status = "PENDING";
   if(!StoreCommand(record)) return false;
   g_ActiveCommand = record;
   g_HasActiveCommand = true;
   return true;
}

void EndActiveCommand()
{
   g_HasActiveCommand = false;
}

void RejectBeforeBrokerMutation(const CommandRecord &record, const string correlationId)
{
   SendNackValues(record.command_id, record.trade_id, record.action,
                  "LOCAL_COMMAND_PERSISTENCE_FAILED", 0,
                  "PENDING command was not durably verified; no broker request was sent",
                  correlationId);
   DisconnectSocket();
}

void CompleteFromTradeResult(CommandRecord &record)
{
   record.retcode = g_Trade.ResultRetcode();
   record.reason = g_Trade.ResultRetcodeDescription();
   record.order_id = UlongText(g_Trade.ResultOrder());
   record.deal_id = UlongText(g_Trade.ResultDeal());
   record.actual_price = g_Trade.ResultPrice();
   if(record.actual_price <= 0) record.actual_price = g_Trade.ResultBid();
   if(g_Trade.ResultVolume() > 0) record.actual_volume = g_Trade.ResultVolume();
   ulong positionIdentifier = 0;
   if(g_Trade.ResultDeal() > 0 && HistoryDealSelect(g_Trade.ResultDeal()))
      positionIdentifier = (ulong)HistoryDealGetInteger(g_Trade.ResultDeal(), DEAL_POSITION_ID);
   record.position_id = UlongText(positionIdentifier);
   ulong ticket = FindTicketByIdentifier(record.position_id);
   record.position_ticket = UlongText(ticket);
}

void HandleOpen(const string &json, CommandRecord &record, const string correlationId)
{
   string reason = "";
   if(!TradingIdentityReady(reason) || !g_BaselineVerified || g_DailyHalted || InpEmergencyStop)
   {
      if(reason == "") reason = !g_BaselineVerified ? "BASELINE_UNVERIFIED" : "ENTRY_HALTED";
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     reason, 0, "open disabled", correlationId); return;
   }
   if(JsonGetString(json, "symbol") != _Symbol || (int)JsonGetLong(json, "magic") != InpMagicNumber)
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "IDENTITY_MISMATCH", 0, "symbol/magic", correlationId); return;
   }
   if(CountAccountPositions() != CountOwnedPositions() ||
      CountAccountPendingOrders() != CountOwnedPendingOrders())
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "FOREIGN_ACCOUNT_EXPOSURE", 0,
                     "account-complete exposure must reconcile before OPEN",
                     correlationId); return;
   }
   if(CountOwnedPositions() + CountOwnedPendingOrders() >= InpMaxOwnedPositions)
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "MAX_OWNED_EXPOSURES", 0, "positions plus pending orders ceiling",
                     correlationId); return;
   }

   MqlTick tick;
   if(!SymbolInfoTick(_Symbol, tick))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "NO_QUOTE", 0, "SymbolInfoTick failed", correlationId); return;
   }
   double price = (record.direction == "BUY") ? tick.ask : tick.bid;
   double reference = JsonGetDouble(json, "reference_price");
   long maxSlippage = JsonGetLong(json, "max_slippage_points");
   if(reference <= 0 || MathAbs(price - reference) / _Point > (double)maxSlippage)
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "MAX_SLIPPAGE_EXCEEDED", 0, "stale reference", correlationId); return;
   }
   double sl = JsonGetDouble(json, "sl");
   double tp1 = JsonGetDouble(json, "tp1");
   double tp2 = JsonGetDouble(json, "tp2");
   double tp3 = JsonGetDouble(json, "tp3");
   if(!ValidateOpenGeometry(record.direction, price, sl, tp1, tp2, tp3, reason))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     reason, 0, "geometry", correlationId); return;
   }

   double volume = NormalizeVolumeDown(JsonGetDouble(json, "volume"));
   if(volume <= 0)
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "VOLUME_BELOW_MINIMUM", 0, "normalised to zero", correlationId); return;
   }
   ENUM_ORDER_TYPE orderType = (record.direction == "BUY") ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   double loss = 0.0;
   if(!OrderCalcProfit(orderType, _Symbol, volume, price, sl, loss))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "RISK_CALC_FAILED", 0, "OrderCalcProfit", correlationId); return;
   }
   double riskBase = MathMin(AccountInfoDouble(ACCOUNT_BALANCE), AccountInfoDouble(ACCOUNT_EQUITY));
   double commandPct = JsonGetDouble(json, "risk_pct_limit");
   double allowedPct = MathMin(InpMaxRiskPct, commandPct);
   double allowedRisk = MathMin(JsonGetDouble(json, "risk_limit"), riskBase * allowedPct / 100.0);
   bool exposureRiskValid = false;
   double existingExposureRisk = OwnedExposureRisk(exposureRiskValid);
   double portfolioRiskCeiling = riskBase * InpMaxRiskPct / 100.0;
   if(!exposureRiskValid || allowedPct <= 0 || allowedRisk <= 0 ||
      MathAbs(loss) > allowedRisk + 0.01 ||
      existingExposureRisk + MathAbs(loss) > portfolioRiskCeiling + 0.01)
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "ACTUAL_RISK_EXCEEDS_LIMIT", 0, DoubleToString(MathAbs(loss), 2), correlationId); return;
   }
   double margin = 0.0;
   if(!OrderCalcMargin(orderType, _Symbol, volume, price, margin) ||
      margin > AccountInfoDouble(ACCOUNT_MARGIN_FREE))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "INSUFFICIENT_MARGIN", 0, DoubleToString(margin, 2), correlationId); return;
   }

   record.actual_volume = volume;
   if(!BeginActiveCommand(record))
   {
      RejectBeforeBrokerMutation(record, correlationId);
      return;
   }
   bool success = false;
   uint retcode = 0;
   string retryValidationReason = "";
   for(int attempt = 0; attempt < InpMaxCommandRetries; attempt++)
   {
      if(CountAccountPositions() != CountOwnedPositions() ||
         CountAccountPendingOrders() != CountOwnedPendingOrders())
      {
         retryValidationReason = "FOREIGN_ACCOUNT_EXPOSURE";
         break;
      }
      if(CountOwnedPositions() + CountOwnedPendingOrders() >= InpMaxOwnedPositions)
      {
         retryValidationReason = "MAX_OWNED_EXPOSURES";
         break;
      }
      if(attempt > 0)
      {
         MqlTick retryTick;
         if(!SymbolInfoTick(_Symbol, retryTick))
         {
            retryValidationReason = "NO_QUOTE";
            break;
         }
         double retryPrice = (record.direction == "BUY") ? retryTick.ask : retryTick.bid;
         double retryLoss = 0.0;
         double retryMargin = 0.0;
         double retryRiskBase = MathMin(AccountInfoDouble(ACCOUNT_BALANCE),
                                        AccountInfoDouble(ACCOUNT_EQUITY));
         double retryAllowedRisk = MathMin(JsonGetDouble(json, "risk_limit"),
                                           retryRiskBase * allowedPct / 100.0);
         bool retryExposureValid = false;
         double retryExposureRisk = OwnedExposureRisk(retryExposureValid);
         if(MathAbs(retryPrice - reference) / _Point > (double)maxSlippage)
            retryValidationReason = "MAX_SLIPPAGE_EXCEEDED";
         else if(!ValidateOpenGeometry(record.direction, retryPrice, sl, tp1, tp2, tp3,
                                       retryValidationReason)) { }
         else if(!retryExposureValid ||
                 !OrderCalcProfit(orderType, _Symbol, volume, retryPrice, sl, retryLoss) ||
                 MathAbs(retryLoss) > retryAllowedRisk + 0.01 ||
                 retryExposureRisk + MathAbs(retryLoss) >
                    retryRiskBase * InpMaxRiskPct / 100.0 + 0.01)
            retryValidationReason = "ACTUAL_RISK_EXCEEDS_LIMIT";
         else if(!OrderCalcMargin(orderType, _Symbol, volume, retryPrice, retryMargin) ||
                 retryMargin > AccountInfoDouble(ACCOUNT_MARGIN_FREE))
            retryValidationReason = "INSUFFICIENT_MARGIN";
         if(retryValidationReason != "") break;
      }
      ResetLastError();
      if(record.direction == "BUY") success = g_Trade.Buy(volume, _Symbol, 0.0, sl, tp3, TradeComment(record.trade_id));
      else success = g_Trade.Sell(volume, _Symbol, 0.0, sl, tp3, TradeComment(record.trade_id));
      retcode = g_Trade.ResultRetcode();
      if(success && IsSuccessRetcode(retcode)) break;
      if(!IsRetriableRetcode(retcode)) break;
      Sleep(100);
   }
   CompleteFromTradeResult(record);
   if(retryValidationReason != "") record.reason = retryValidationReason;
   EndActiveCommand();
   if(success && IsSuccessRetcode(record.retcode))
   {
      record.status = (record.retcode == TRADE_RETCODE_PLACED) ? "ACCEPTED" :
                      ((record.retcode == TRADE_RETCODE_DONE_PARTIAL) ? "PARTIAL_ACK" : "ACK");
      if(!StoreCommand(record)) { DisconnectSocket(); return; }
      SendAck(record, correlationId);
   }
   else
   {
      record.status = "NACK";
      if(!StoreCommand(record)) { DisconnectSocket(); return; }
      SendNackValues(record.command_id, record.trade_id, record.action,
                     (retryValidationReason == "" ? "BROKER_REJECTED" : "RETRY_VALIDATION_FAILED"),
                     record.retcode, record.reason, correlationId);
   }
}

bool PositionForCommand(const string &json, ulong &ticket, string &reason)
{
   string positionId = JsonGetString(json, "position_id");
   string ticketText = JsonGetString(json, "position_ticket");
   ticket = ResolveTicket(positionId, ticketText);
   if(!SelectOwnedPosition(ticket)) { reason = "POSITION_NOT_FOUND_OR_NOT_OWNED"; return false; }
   if(UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER)) != positionId)
   { reason = "POSITION_ID_MISMATCH"; return false; }
   return ValidatePositionVersion(JsonGetLong(json, "expected_position_version"), reason);
}

void HandleModifySl(const string &json, CommandRecord &record, const string correlationId)
{
   string reason = ""; ulong ticket = 0;
   if(!TradingIdentityReady(reason) || !PositionForCommand(json, ticket, reason))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     reason, 0, "modify validation", correlationId); return;
   }
   ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   double oldSl = PositionGetDouble(POSITION_SL);
   double brokerFinalTp = PositionGetDouble(POSITION_TP);
   double suppliedFinalTp = JsonGetDouble(json, "final_tp");
   double tickSize = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(suppliedFinalTp <= 0 || !PriceAligned(suppliedFinalTp) ||
      (brokerFinalTp > 0 && MathAbs(brokerFinalTp - suppliedFinalTp) > tickSize / 2.0))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "FINAL_TP_MISMATCH", 0, "broker TP preserved", correlationId); return;
   }
   if(brokerFinalTp <= 0) brokerFinalTp = suppliedFinalTp;
   double newSl = JsonGetDouble(json, "new_sl");
   MqlTick tick;
   if(!SymbolInfoTick(_Symbol, tick))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "NO_QUOTE", 0, "modify quote unavailable", correlationId); return;
   }
   double exitPrice = (type == POSITION_TYPE_BUY) ? tick.bid : tick.ask;
   double minimum = (double)MathMax(SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL),
                                    SymbolInfoInteger(_Symbol, SYMBOL_TRADE_FREEZE_LEVEL)) * _Point;
   bool improves = (type == POSITION_TYPE_BUY) ?
      ((oldSl <= 0 || newSl > oldSl + tickSize / 2.0) && newSl <= exitPrice - minimum &&
       newSl < brokerFinalTp) :
      ((oldSl <= 0 || newSl < oldSl - tickSize / 2.0) && newSl >= exitPrice + minimum &&
       newSl > brokerFinalTp);
   if(newSl <= 0 || !PriceAligned(newSl) || !improves)
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "INVALID_SL_MODIFICATION", 0, "not improving/executable", correlationId); return;
   }
   record.direction = (type == POSITION_TYPE_BUY) ? "BUY" : "SELL";
   record.position_id = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
   record.position_ticket = UlongText(ticket);
   record.expected_position_version = JsonGetLong(json, "expected_position_version");
   record.target_sl = newSl;
   record.final_tp = brokerFinalTp;
   if(!BeginActiveCommand(record))
   {
      RejectBeforeBrokerMutation(record, correlationId);
      return;
   }
   bool success = g_Trade.PositionModify(ticket, newSl, brokerFinalTp);
   CompleteFromTradeResult(record);
   record.position_id = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
   record.position_ticket = UlongText(ticket);
   EndActiveCommand();
   if(success && IsSuccessRetcode(record.retcode))
   { record.status = (record.retcode == TRADE_RETCODE_PLACED) ? "ACCEPTED" :
                     ((record.retcode == TRADE_RETCODE_DONE_PARTIAL) ? "PARTIAL_ACK" : "ACK");
     if(!StoreCommand(record)) { DisconnectSocket(); return; }
     SendAck(record, correlationId); }
   else
   { record.status = "NACK";
     if(!StoreCommand(record)) { DisconnectSocket(); return; }
     SendNackValues(record.command_id, record.trade_id,
      record.action, "BROKER_REJECTED", record.retcode, record.reason, correlationId); }
}

ENUM_ORDER_TYPE_FILLING SymbolFilling()
{
   long flags = SymbolInfoInteger(_Symbol, SYMBOL_FILLING_MODE);
   if((flags & SYMBOL_FILLING_IOC) == SYMBOL_FILLING_IOC) return ORDER_FILLING_IOC;
   if((flags & SYMBOL_FILLING_FOK) == SYMBOL_FILLING_FOK) return ORDER_FILLING_FOK;
   return ORDER_FILLING_RETURN;
}

bool PartialCloseNetting(const ulong ticket, const double volume, MqlTradeResult &result)
{
   if(!SelectOwnedPosition(ticket)) return false;
   ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   MqlTick tick; if(!SymbolInfoTick(_Symbol, tick)) return false;
   MqlTradeRequest request;
   ZeroMemory(request); ZeroMemory(result);
   request.action = TRADE_ACTION_DEAL;
   request.magic = InpMagicNumber;
   request.position = ticket;
   request.symbol = _Symbol;
   request.volume = volume;
   request.type = (type == POSITION_TYPE_BUY) ? ORDER_TYPE_SELL : ORDER_TYPE_BUY;
   request.price = (type == POSITION_TYPE_BUY) ? tick.bid : tick.ask;
   request.deviation = InpSlippagePoints;
   request.type_filling = SymbolFilling();
   request.comment = "QSX|PARTIAL";
   return OrderSend(request, result);
}

void HandlePartialClose(const string &json, CommandRecord &record, const string correlationId)
{
   string reason = ""; ulong ticket = 0;
   if(!TradingIdentityReady(reason) || !PositionForCommand(json, ticket, reason))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     reason, 0, "partial validation", correlationId); return;
   }
   double current = PositionGetDouble(POSITION_VOLUME);
   double closeVolume = NormalizeVolumeDown(JsonGetDouble(json, "close_volume"));
   double minimum = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double remaining = NormalizeDouble(current - closeVolume, LotDigits(SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP)));
   if(closeVolume <= 0 || closeVolume >= current || remaining < minimum)
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     "PARTIAL_DUST_OR_FULL_CLOSE", 0, "use virtual milestone/full close", correlationId); return;
   }
   ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   record.direction = (type == POSITION_TYPE_BUY) ? "BUY" : "SELL";
   record.position_id = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
   record.position_ticket = UlongText(ticket);
   record.actual_volume = closeVolume;
   record.expected_position_version = JsonGetLong(json, "expected_position_version");
   if(!BeginActiveCommand(record))
   {
      RejectBeforeBrokerMutation(record, correlationId);
      return;
   }
   bool success = false;
   uint retcode = 0; ulong order = 0, deal = 0;
   double resultPrice = 0, resultVolume = 0;
   for(int attempt = 0; attempt < InpMaxCommandRetries; attempt++)
   {
      if(AccountMarginModeText() == "HEDGING")
      {
         success = g_Trade.PositionClosePartial(ticket, closeVolume, InpSlippagePoints);
         retcode = g_Trade.ResultRetcode(); order = g_Trade.ResultOrder();
         deal = g_Trade.ResultDeal(); resultPrice = g_Trade.ResultPrice();
         resultVolume = g_Trade.ResultVolume();
         record.reason = g_Trade.ResultRetcodeDescription();
      }
      else
      {
         MqlTradeResult result;
         success = PartialCloseNetting(ticket, closeVolume, result);
         retcode = result.retcode; order = result.order; deal = result.deal;
         resultPrice = result.price; resultVolume = result.volume;
         record.reason = result.comment;
      }
      if(success && IsSuccessRetcode(retcode)) break;
      if(!IsRetriableRetcode(retcode)) break;
      Sleep(100);
   }
   record.retcode = retcode; record.order_id = UlongText(order); record.deal_id = UlongText(deal);
   record.actual_price = resultPrice;
   if(resultVolume > 0) record.actual_volume = resultVolume;
   EndActiveCommand();
   if(success && IsSuccessRetcode(retcode))
   { record.status = (retcode == TRADE_RETCODE_PLACED) ? "ACCEPTED" :
                     ((retcode == TRADE_RETCODE_DONE_PARTIAL) ? "PARTIAL_ACK" : "ACK");
     if(!StoreCommand(record)) { DisconnectSocket(); return; }
     SendAck(record, correlationId); }
   else
   { record.status = "NACK";
     if(!StoreCommand(record)) { DisconnectSocket(); return; }
     SendNackValues(record.command_id, record.trade_id,
      record.action, "BROKER_REJECTED", retcode, record.reason, correlationId); }
}

void HandleClose(const string &json, CommandRecord &record, const string correlationId)
{
   string reason = ""; ulong ticket = 0;
   if(!TradingIdentityReady(reason) || !PositionForCommand(json, ticket, reason))
   {
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     reason, 0, "close validation", correlationId); return;
   }
   ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   record.direction = (type == POSITION_TYPE_BUY) ? "BUY" : "SELL";
   record.position_id = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
   record.position_ticket = UlongText(ticket);
   record.actual_volume = PositionGetDouble(POSITION_VOLUME);
   record.expected_position_version = JsonGetLong(json, "expected_position_version");
   if(!BeginActiveCommand(record))
   {
      RejectBeforeBrokerMutation(record, correlationId);
      return;
   }
   bool success = false;
   for(int attempt = 0; attempt < InpMaxCommandRetries; attempt++)
   {
      success = g_Trade.PositionClose(ticket, InpSlippagePoints);
      uint retcode = g_Trade.ResultRetcode();
      if(success && IsSuccessRetcode(retcode)) break;
      if(!IsRetriableRetcode(retcode)) break;
      Sleep(100);
   }
   CompleteFromTradeResult(record);
   record.position_id = JsonGetString(json, "position_id");
   record.position_ticket = UlongText(ticket);
   EndActiveCommand();
   if(success && IsSuccessRetcode(record.retcode))
   { record.status = (record.retcode == TRADE_RETCODE_PLACED) ? "ACCEPTED" :
                     ((record.retcode == TRADE_RETCODE_DONE_PARTIAL) ? "PARTIAL_ACK" : "ACK");
     if(!StoreCommand(record)) { DisconnectSocket(); return; }
     SendAck(record, correlationId); }
   else
   { record.status = "NACK";
     if(!StoreCommand(record)) { DisconnectSocket(); return; }
     SendNackValues(record.command_id, record.trade_id,
      record.action, "BROKER_REJECTED", record.retcode, record.reason, correlationId); }
}

bool CloseAllOwned(const string commandId, const string tradeId, CommandRecord &record)
{
   bool allSuccess = true;
   record.retcode = TRADE_RETCODE_DONE;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(!SelectOwnedPosition(ticket)) continue;
      ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      record.direction = (type == POSITION_TYPE_BUY) ? "BUY" : "SELL";
      record.position_id = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
      record.position_ticket = UlongText(ticket);
      g_ActiveCommand = record;
      bool success = false;
      for(int attempt = 0; attempt < InpMaxCommandRetries; attempt++)
      {
         success = g_Trade.PositionClose(ticket, InpSlippagePoints);
         uint closeRetcode = g_Trade.ResultRetcode();
         if(success && IsSuccessRetcode(closeRetcode)) break;
         if(!IsRetriableRetcode(closeRetcode)) break;
         Sleep(100);
      }
      if(!success || !IsSuccessRetcode(g_Trade.ResultRetcode()))
      {
         allSuccess = false; record.retcode = g_Trade.ResultRetcode();
         record.reason = g_Trade.ResultRetcodeDescription();
      }
      else if(FindTicketByIdentifier(record.position_id) != 0)
      {
         allSuccess = false; record.retcode = TRADE_RETCODE_DONE_PARTIAL;
         record.reason = "PARTIAL_CLOSE_REMAINS";
      }
   }
   return allSuccess;
}

void HandleCloseAll(const string &json, CommandRecord &record, const string correlationId)
{
   string reason = "";
   if(!TradingIdentityReady(reason) || JsonGetString(json, "symbol") != _Symbol ||
      (int)JsonGetLong(json, "magic") != InpMagicNumber)
   {
      if(reason == "") reason = "IDENTITY_MISMATCH";
      RejectAndStore(record.command_id, record.trade_id, record.action, record.direction,
                     reason, 0, "close-all validation", correlationId); return;
   }
   if(!BeginActiveCommand(record))
   {
      RejectBeforeBrokerMutation(record, correlationId);
      return;
   }
   bool success = CloseAllOwned(record.command_id, record.trade_id, record);
   EndActiveCommand();
   if(success)
   {
      record.status = "ACK"; record.reason = "ALL_OWNED_POSITIONS_CLOSED";
      record.order_id = "0"; record.deal_id = "0"; record.position_id = "0";
      record.position_ticket = "0"; record.actual_price = 0; record.actual_volume = 0;
      if(!StoreCommand(record)) { DisconnectSocket(); return; }
      SendAck(record, correlationId);
   }
   else
   {
      record.status = "NACK";
      if(!StoreCommand(record)) { DisconnectSocket(); return; }
      SendNackValues(record.command_id, record.trade_id, record.action,
                     "BROKER_REJECTED", record.retcode, record.reason, correlationId);
   }
}

bool ValidateCommandShape(const string &json, const string action, string &reason)
{
   string knownFields[] = {
      "command_id", "trade_id", "action", "expires_at_ms", "reservation_id",
      "symbol", "magic", "direction", "volume", "reference_price",
      "max_slippage_points", "sl", "tp1", "tp2", "tp3", "risk_limit",
      "risk_pct_limit", "comment", "position_id", "position_ticket",
      "expected_position_version", "new_sl", "final_tp", "close_volume",
      "deal_cursor"
   };
   for(int i = 0; i < ArraySize(knownFields); i++)
   {
      int count = JsonKeyCount(json, knownFields[i]);
      if(count > 1)
      {
         reason = "DUPLICATE_FIELD:" + knownFields[i];
         return false;
      }
   }
   if(!RequireUniqueField(json, "command_id", reason) ||
      !RequireUniqueField(json, "trade_id", reason) ||
      !RequireUniqueField(json, "action", reason) ||
      !RequireUniqueField(json, "expires_at_ms", reason)) return false;

   if(action == "OPEN")
   {
      string required[] = {
         "reservation_id", "symbol", "magic", "direction", "volume",
         "reference_price", "max_slippage_points", "sl", "tp1", "tp2",
         "tp3", "risk_limit", "risk_pct_limit", "comment"
      };
      for(int i = 0; i < ArraySize(required); i++)
         if(!RequireUniqueField(json, required[i], reason)) return false;
   }
   else if(action == "MODIFY_SL")
   {
      string required[] = {"position_id", "position_ticket", "new_sl", "final_tp",
                           "expected_position_version"};
      for(int i = 0; i < ArraySize(required); i++)
         if(!RequireUniqueField(json, required[i], reason)) return false;
   }
   else if(action == "PARTIAL_CLOSE")
   {
      string required[] = {"position_id", "position_ticket", "close_volume",
                           "expected_position_version"};
      for(int i = 0; i < ArraySize(required); i++)
         if(!RequireUniqueField(json, required[i], reason)) return false;
   }
   else if(action == "CLOSE")
   {
      string required[] = {"position_id", "position_ticket", "expected_position_version"};
      for(int i = 0; i < ArraySize(required); i++)
         if(!RequireUniqueField(json, required[i], reason)) return false;
   }
   else if(action == "CLOSE_ALL")
   {
      if(!RequireUniqueField(json, "symbol", reason) ||
         !RequireUniqueField(json, "magic", reason)) return false;
   }
   else if(action == "REQUEST_SNAPSHOT")
   {
      if(!RequireUniqueField(json, "deal_cursor", reason)) return false;
   }
   return true;
}

void HandleCommand(const string &json)
{
   string commandId = JsonGetString(json, "command_id");
   string tradeId = JsonGetString(json, "trade_id");
   string action = JsonGetString(json, "action");
   string correlationId = commandId;
   string shapeReason = "";
   if(!ValidateCommandShape(json, action, shapeReason) ||
      commandId == "" || tradeId == "" || action == "")
   {
      SendNackValues(commandId, tradeId, action, "INVALID_COMMAND_SHAPE", 0,
                     shapeReason, correlationId);
      return;
   }
   if(action == "OPEN" && AccountMarginModeText() != "HEDGING")
   {
      RejectAndStore(commandId, tradeId, action, JsonGetString(json, "direction"),
                     "HEDGING_ACCOUNT_REQUIRED", 0,
                     "new entries are forbidden on netting accounts", correlationId);
      return;
   }
   if(action != "REQUEST_SNAPSHOT")
   {
      if(ReplayCommandIfKnown(commandId, tradeId, action, correlationId)) return;
   }
   if(JsonGetLong(json, "expires_at_ms") <= UtcNowMs())
   {
      RejectAndStore(commandId, tradeId, action, JsonGetString(json, "direction"),
                     "COMMAND_EXPIRED", 0, "expiry", correlationId); return;
   }
   CommandRecord record;
   record.command_id = commandId; record.trade_id = tradeId; record.action = action;
   record.direction = JsonGetString(json, "direction"); record.status = "PENDING";
   record.reason = ""; record.retcode = 0; record.order_id = "0"; record.deal_id = "0";
   record.position_id = JsonGetString(json, "position_id");
   record.position_ticket = JsonGetString(json, "position_ticket");
   record.actual_price = 0; record.actual_volume = 0;
   record.expected_position_version = 0; record.target_sl = 0.0; record.final_tp = 0.0;

   if(action == "OPEN") HandleOpen(json, record, correlationId);
   else if(action == "MODIFY_SL") HandleModifySl(json, record, correlationId);
   else if(action == "PARTIAL_CLOSE") HandlePartialClose(json, record, correlationId);
   else if(action == "CLOSE") HandleClose(json, record, correlationId);
   else if(action == "CLOSE_ALL") HandleCloseAll(json, record, correlationId);
   else if(action == "REQUEST_SNAPSHOT")
   {
      long cursorTime = 0; ulong cursorDeal = 0;
      string requestedCursor = JsonGetString(json, "deal_cursor");
      if(!ParseDealCursor(requestedCursor, cursorTime, cursorDeal))
         RejectAndStore(commandId, tradeId, action, record.direction,
                        "INVALID_DEAL_CURSOR", 0, requestedCursor, correlationId);
      else
      {
         if(SendFullSnapshot(correlationId, requestedCursor))
         {
            record.status = "ACK"; record.reason = "SNAPSHOT_SENT";
            if(!StoreCommand(record)) { DisconnectSocket(); return; }
            SendAck(record, correlationId);
         }
         else
            RejectAndStore(commandId, tradeId, action, record.direction,
                           "SNAPSHOT_SEND_FAILED", 0, "atomic snapshot incomplete",
                           correlationId);
      }
   }
   else RejectAndStore(commandId, tradeId, action, record.direction,
                       "UNKNOWN_ACTION", 0, action, correlationId);
}

//+------------------------------------------------------------------+
//| Session handshake and command routing                            |
//+------------------------------------------------------------------+
void HandleHelloAck(const string &json)
{
   string shapeReason = "";
   string required[] = {"status", "server_id", "config_hash", "expected_account_id",
                        "expected_broker_server", "expected_account_trade_mode",
                        "expected_symbol", "expected_magic", "allow_real", "deal_cursor"};
   for(int i = 0; i < ArraySize(required); i++)
   {
      if(!RequireUniqueField(json, required[i], shapeReason))
      {
         Print("[QSX4] HELLO_ACK rejected: ", shapeReason);
         DisconnectSocket();
         return;
      }
   }
   string allowRealRaw = JsonGetRaw(json, "allow_real");
   if(allowRealRaw != "true" && allowRealRaw != "false")
   {
      Print("[QSX4] HELLO_ACK rejected: INVALID_ALLOW_REAL");
      DisconnectSocket();
      return;
   }
   string status = JsonGetString(json, "status");
   string expectedAccount = JsonGetString(json, "expected_account_id");
   string expectedBrokerServer = JsonGetString(json, "expected_broker_server");
   string expectedAccountMode = JsonGetString(json, "expected_account_trade_mode");
   string expectedSymbol = JsonGetString(json, "expected_symbol");
   int expectedMagic = (int)JsonGetLong(json, "expected_magic");
   bool allowReal = JsonGetBool(json, "allow_real");
   string configHash = JsonGetString(json, "config_hash");
   string peerDealCursor = JsonGetString(json, "deal_cursor");
   long peerCursorTime = 0;
   ulong peerCursorDeal = 0;
   bool identityOk = status == "ACCEPTED" && expectedAccount == AccountId() &&
                     expectedBrokerServer == AccountInfoString(ACCOUNT_SERVER) &&
                     expectedAccountMode == AccountTradeModeText() &&
                     expectedSymbol == _Symbol && expectedMagic == InpMagicNumber &&
                     configHash == InpConfigHash &&
                     ParseDealCursor(peerDealCursor, peerCursorTime, peerCursorDeal);
   bool modeOk = AccountTradeModeText() != "REAL" || (allowReal && InpPermitRealAccount);
   bool permissionOk = AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) &&
                       AccountInfoInteger(ACCOUNT_TRADE_EXPERT) &&
                       TerminalInfoInteger(TERMINAL_TRADE_ALLOWED);
   if(!identityOk || !modeOk || !permissionOk)
   {
      Print("[QSX4] HELLO_ACK rejected identity/mode/permissions; session closed");
      DisconnectSocket();
      return;
   }
   g_Authenticated = true;
   Print("[QSX4] Authenticated broker session accepted for ", AccountTradeModeText(), " account ", AccountId());
   ResetBarPublicationState();
   if(!SendFullSnapshot(JsonGetString(json, "msg_id"), peerDealCursor))
   {
      Print("[QSX4] Initial atomic snapshot failed; session closed for clean replay");
      DisconnectSocket();
      return;
   }
   if(InpSendHistoryOnConnect) SendHistory();
   else SeedBarPublicationState();
}

void ProcessInbound(const string &json)
{
   string type = ""; string reason = "";
   if(!ValidateIncomingEnvelope(json, type, reason))
   {
      Print("[QSX4] Inbound frame rejected: ", reason);
      if(reason == "AUTH_FAILED" || reason == "EPOCH_MISMATCH" || reason == "PROTOCOL_VERSION")
         DisconnectSocket();
      return;
   }
   if(type == "HELLO_ACK") HandleHelloAck(json);
   else if(type == "COMMAND") HandleCommand(json);
}

bool ConnectSocket()
{
   if(!g_LocalStateHealthy) return false;
   DisconnectSocket();
   g_Socket = SocketCreate();
   if(g_Socket == INVALID_HANDLE) return false;
   if(!SocketConnect(g_Socket, InpHost, InpPort, 3000))
   {
      DisconnectSocket(); return false;
   }
   g_Connected = true;
   g_Authenticated = false;
   g_ConnectionEpoch = NewEpoch();
   g_SendSequence = 0;
   g_LastRecvSequence = 0;
   g_ReceiveBuffer = "";
   SendHello();
   return true;
}

//+------------------------------------------------------------------+
//| Authoritative broker transaction events                          |
//+------------------------------------------------------------------+
void EmitBrokerEvent(const string eventType, const string eventId,
                     const CommandRecord &context, const string orderId,
                     const string dealId, const string positionId,
                     const string positionTicket, const string direction,
                     const double price, const double volumeDelta,
                     const double remainingVolume, const double sl,
                     const double tp, const double profit,
                     const double commission, const double swap,
                     const double fee, const string reason,
                     const long brokerTimeMs, const long positionVersion)
{
   if(!g_Authenticated) return;
   string fields = ",\"event_id\":" + J(eventId) +
      ",\"event_type\":" + J(eventType) +
      ",\"command_id\":" + J(context.command_id) +
      ",\"trade_id\":" + J(context.trade_id) +
      ",\"order_id\":" + J(orderId) + ",\"deal_id\":" + J(dealId) +
      ",\"position_id\":" + J(positionId) +
      ",\"position_ticket\":" + J(positionTicket) +
      ",\"symbol\":" + J(_Symbol) + ",\"magic\":" + IntegerToString(InpMagicNumber) +
      ",\"direction\":" + J(direction) +
      ",\"price\":" + DoubleToString(price, _Digits) +
      ",\"volume_delta\":" + DoubleToString(volumeDelta, 8) +
      ",\"remaining_volume\":" + DoubleToString(remainingVolume, 8) +
      ",\"sl\":" + DoubleToString(sl, _Digits) +
      ",\"tp\":" + DoubleToString(tp, _Digits) +
      ",\"profit\":" + DoubleToString(profit, 2) +
      ",\"commission\":" + DoubleToString(commission, 2) +
      ",\"swap\":" + DoubleToString(swap, 2) +
      ",\"fee\":" + DoubleToString(fee, 2) +
      ",\"reason\":" + J(reason) +
      ",\"broker_time_ms\":" + LongText(brokerTimeMs) +
      ",\"position_version\":" + LongText(positionVersion);
   SendMessage("BROKER_EVENT", context.command_id, fields);
}

void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
{
   string orderId = UlongText(trans.order);
   string dealId = UlongText(trans.deal);
   string positionId = UlongText(trans.position);

   if(trans.type == TRADE_TRANSACTION_DEAL_ADD)
   {
      if(!DealIsOwned(trans.deal)) return;
      if(orderId == "0")
         orderId = UlongText((ulong)HistoryDealGetInteger(trans.deal, DEAL_ORDER));
      positionId = UlongText((ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID));
      long brokerTime = (long)HistoryDealGetInteger(trans.deal, DEAL_TIME_MSC);
      string ticketText = "0"; double sl = 0, tp = 0; long version = brokerTime;
      string direction = DealDirection(trans.deal);
      double remaining = RemainingVolumeByIdentifier(positionId, ticketText, sl, tp, version, direction);
      version = PositionVersionForIdentity(positionId, brokerTime, true);
      if(version < 1) { DisconnectSocket(); return; }
      ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
      double volume = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);
      double delta = (entry == DEAL_ENTRY_IN) ? volume :
                     ((entry == DEAL_ENTRY_INOUT) ? 0.0 : -volume);
      string eventType = (entry == DEAL_ENTRY_IN) ? "POSITION_OPENED" :
                         ((entry == DEAL_ENTRY_INOUT) ? "POSITION_REVERSED" :
                          ((remaining <= 0.0) ? "POSITION_CLOSED" : "PARTIAL_FILLED"));
      CommandRecord context;
      ContextForIdentity(orderId, dealId, positionId, context);
      if(context.direction != "" && context.direction != "UNKNOWN") direction = context.direction;
      else context.direction = direction;
      if(context.command_id != "BROKER")
      {
         if(orderId != "0") context.order_id = orderId;
         context.deal_id = dealId;
         context.position_id = positionId;
         if(ticketText != "0") context.position_ticket = ticketText;
         context.actual_price = HistoryDealGetDouble(trans.deal, DEAL_PRICE);
         context.actual_volume = volume;
         if(context.status == "ACCEPTED" &&
            (entry == DEAL_ENTRY_IN || entry == DEAL_ENTRY_INOUT)) context.status = "ACK";
         if(!StoreCommand(context)) { DisconnectSocket(); return; }
      }
      string eventId = "DEAL-" + dealId;
      EmitBrokerEvent(eventType, eventId, context, orderId, dealId, positionId,
         ticketText, direction, HistoryDealGetDouble(trans.deal, DEAL_PRICE), delta,
         remaining, sl, tp, HistoryDealGetDouble(trans.deal, DEAL_PROFIT),
         HistoryDealGetDouble(trans.deal, DEAL_COMMISSION),
         HistoryDealGetDouble(trans.deal, DEAL_SWAP),
         HistoryDealGetDouble(trans.deal, DEAL_FEE), DealReasonText(trans.deal),
         brokerTime, version);
      if(!AdvanceLocalDealCursor(brokerTime, trans.deal)) DisconnectSocket();
      return;
   }

   if(trans.type == TRADE_TRANSACTION_POSITION && trans.symbol == _Symbol)
   {
      ulong ticket = trans.position;
      if(!SelectOwnedPosition(ticket)) return;
      positionId = UlongText((ulong)PositionGetInteger(POSITION_IDENTIFIER));
      long brokerUpdateTime = (long)PositionGetInteger(POSITION_TIME_UPDATE_MSC);
      long version = PositionVersionForIdentity(positionId, brokerUpdateTime, true);
      if(version < 1) { DisconnectSocket(); return; }
      CommandRecord context;
      ContextForIdentity(orderId, dealId, positionId, context);
      string direction = ((ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY) ? "BUY" : "SELL";
      EmitBrokerEvent("POSITION_UPDATED", "POSITION-" + positionId + "-" + LongText(version),
         context, orderId, dealId, positionId, UlongText(ticket), direction,
         PositionGetDouble(POSITION_PRICE_CURRENT), 0.0, PositionGetDouble(POSITION_VOLUME),
         PositionGetDouble(POSITION_SL), PositionGetDouble(POSITION_TP), 0, 0, 0, 0,
         "POSITION_UPDATE", UtcNowMs(), version);
   }
}

//+------------------------------------------------------------------+
//| Terminal lifecycle                                               |
//+------------------------------------------------------------------+
int OnInit()
{
   bool baselineApprovalDaySet =
      InpApproveBaselineUtcDay != "";

   bool baselineApprovalEquitySet =
      InpApproveBaselineEquity > 0.0;

   if(baselineApprovalDaySet != baselineApprovalEquitySet ||
      InpApproveBaselineEquity < 0.0 ||
      !IsLoopbackHost(InpHost) || InpAuthToken == "" || InpConfigHash == "" ||
      InpConfigHash == "UNCONFIGURED" ||
      InpPort < 1 || InpPort > 65535 || InpReconnectSec < 1 ||
      InpMaxFrameBytes < 512 || InpMaxFrameBytes > 65536 ||
      InpMaxCommandRetries < 1 || InpMaxCommandRetries > 3 ||
      InpDealReplayLookbackDays < 1 || InpDealReplayLookbackDays > 3650 ||
      InpMagicNumber <= 0 || InpMaxOwnedPositions < 1 || InpMaxOwnedPositions > 3 ||
      InpSlippagePoints < 0 ||
      InpMaxRiskPct <= 0 || InpMaxRiskPct > 1.0 ||
      InpDailyMaxDrawPct <= 0 || InpDailyMaxDrawPct > 3.0)
   {
      Print("[QSX4] Invalid secure configuration; EA will not start");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(AccountTradeModeText() == "REAL" && !InpPermitRealAccount)
   {
      Print("[QSX4] Real account blocked locally");
      return INIT_PARAMETERS_INCORRECT;
   }
   g_Trade.SetExpertMagicNumber(InpMagicNumber);
   g_Trade.SetDeviationInPoints(InpSlippagePoints);
   g_Trade.SetTypeFillingBySymbol(_Symbol);
   g_Trade.SetAsyncMode(false);
   if(!LoadCommandCache())
   {
      MarkLocalStateFailure("COMMAND_JOURNAL_LOAD_FAILED");
      return INIT_FAILED;
   }
   if(!LoadDealCursorState())
   {
      MarkLocalStateFailure("DEAL_CURSOR_TORN_OR_UNVERIFIED");
      return INIT_FAILED;
   }
   if(!UpdateUtcRiskDay()) return INIT_FAILED;
   EventSetTimer(1);
   ConnectSocket();
   Print("[QSX4] Broker adapter initialized in fail-closed mode");
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   DisconnectSocket();
   Print("[QSX4] Broker adapter deinitialized: ", reason);
}

void OnTick()
{
   if(!g_Authenticated) return;
   MqlTick tick;
   if(!SymbolInfoTick(_Symbol, tick)) return;
   string fields = ",\"symbol\":" + J(_Symbol) +
      ",\"bid\":" + DoubleToString(tick.bid, _Digits) +
      ",\"ask\":" + DoubleToString(tick.ask, _Digits) +
      ",\"spread_points\":" + DoubleToString((tick.ask - tick.bid) / _Point, 1) +
      ",\"volume\":" + LongText((long)tick.volume) +
      ",\"broker_time_ms\":" + LongText((long)tick.time_msc);
   SendMessage("TICK", "", fields);
}

void OnTimer()
{
   if(!UpdateUtcRiskDay())
   {
      DisconnectSocket();
      return;
   }
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   g_DailyPnL = equity - g_DayStartEquity;
   if(g_BaselineVerified && !g_DailyHalted && g_DayStartEquity > 0 &&
      (g_DayStartEquity - equity) / g_DayStartEquity * 100.0 >= InpDailyMaxDrawPct)
   {
      if(!SetDailyHalt(true))
      {
         DisconnectSocket();
         return;
      }
      // Canonical daily halt blocks new risk; it does not imply liquidation.
      // Existing positions retain broker-native SL/TP protection.  Any separate
      // liquidation policy must issue a durable authenticated Python command;
      // the EA must never invent an untracked command_id.
      Print("[QSX4] Daily halt set; new risk blocked, existing broker protection retained");
   }

   if(g_Connected && !SocketIsConnected(g_Socket)) DisconnectSocket();

   if(!g_Connected && g_LocalStateHealthy)
   {
      g_ReconnectTicks++;
      if(g_ReconnectTicks >= InpReconnectSec)
      {
         g_ReconnectTicks = 0;
         ConnectSocket();
      }
      return;
   }

   if(g_Authenticated)
   {
      SendHeartbeat();
      SendCompletedBarUpdates();
   }
   string line = ReadLine();
   while(line != "")
   {
      ProcessInbound(line);
      if(!g_Connected) break;
      line = ReadLine();
   }
}
//+------------------------------------------------------------------+
