using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ServerPricetagBBFarma.Code.Util.Log
{
    public class SDLog
    {
        /* private static void LogInfo(string logFile, Dictionary<string, object> logData)
         {
            JsonSettings s = JsonSettings.Instance;
            if (s.LogEnabled == false)
            {
                return;
            }

            TextLog.L
                using (StreamWriter w = File.AppendText(logFilePath))
                {
                    var en = logData.GetEnumerator();
                    while (en.MoveNext())
                    {
                        var e = en.Current;
                        LogObject(w, e);
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }

        private static void LogObject(StreamWriter w, KeyValuePair<string, object> kv)
        {
            string key = kv.Key;
            object obj = kv.Value;
            if (obj == null)
            {
                w.WriteLine(key + ": null");
                return;
            }
            switch (Type.GetTypeCode(obj.GetType()))
            {
                case TypeCode.String:
                    w.WriteLine(key + ": " + obj.ToString());
                    break;
                case TypeCode.DateTime:
                    w.WriteLine(key + ": " + obj.ToString());
                    break;
                case TypeCode.Boolean:
                    w.WriteLine(key + ": " + obj.ToString());
                    break;
                case TypeCode.Object:
                    if (obj.GetType() == typeof(List<String>))
                    {
                        printList(w, key, (List<String>)obj);
                    }
                    else if (obj.GetType() == typeof(EasyLookService.ArrayOfString))
                    {
                        printList(w, key, (List<String>)obj);
                    }
                    else
                    {
                        w.WriteLine(key + ": " + obj.ToString());
                    }
                    break;
                default:
                    w.WriteLine(key + ": " + obj.ToString());
                    break;

            }
        }

        private static void printList(StreamWriter w, string key, List<string> result)
        {
            if (result == null)
            {
                return;
            }
            var en = result.GetEnumerator();
            while (en.MoveNext())
            {
                string s = en.Current;
                w.WriteLine(key + ": " + s);
            }
        }
        */

        public static void LogFailRequest(string requestStr, string responseStr, Exception ex)
        {
            JsonSettings s = JsonSettings.Instance;
            if (s.LogRequestresponseEnabled == false)
            {
                return;
            }

            object reqObj = JsonConvert.DeserializeObject(requestStr);
            var reqStrForm = JsonConvert.SerializeObject(reqObj, Formatting.Indented,
                new JsonConverter[] { new StringEnumConverter() });

            var resStrForm = "";
            if (!string.IsNullOrEmpty(responseStr))
            {
                object resObj = JsonConvert.DeserializeObject(responseStr);
                resStrForm = JsonConvert.SerializeObject(resObj, Formatting.Indented,
                    new JsonConverter[] { new StringEnumConverter() });
            }

            Dictionary<string, string> logData = new Dictionary<string, string>();
            logData["request"] = reqStrForm;
            logData["response"] = resStrForm;
            logData["responseerror"] = ex.Message;
            TextLog.LogOtherDict(s.WebsitePath + "log_failreq.log", logData);
        }

        public static void LogSQLOperation(string name, string sqlString, SqlParameterCollection parameters)
        {
            JsonSettings s = JsonSettings.Instance;
            if (s.LogSqlEnabled == false)
            {
                return;
            }

            Dictionary<string, string> logData = new Dictionary<string, string>();
            SqlParameter[] sqlParams = new SqlParameter[parameters.Count];
            parameters.CopyTo(sqlParams, 0);
            logData["sqlStr"] = sqlString;
            for (int i = 0; i < parameters.Count; i++)
            {
                logData["paramName" + i] = parameters[i].ParameterName;
                logData["paramVal" + i] = parameters[i].Value.ToString();
            }
            TextLog.LogOtherDict(s.WebsitePath + "log_sql.log", logData);
        }
        /*
        public static void LogMagicLinkCall(string clientName, string methodName, Dictionary<string, object> inputPars, Dictionary<string, object> outputPars)
        {
            JsonSettings s = JsonSettings.Instance;
            if (s.LogMagiclinkEnabled == false)
            {
                return;
            }

            Dictionary<string, object> logData = new Dictionary<string, object>();
            logData["clientName"] = clientName;
            logData["methodName"] = methodName;
            foreach (var inpPars in inputPars)
            {
                logData.Add("inputpar_" + inpPars.Key, inpPars.Value);
            }
            foreach (var inpPars in outputPars)
            {
                logData.Add("outputpar_" + inpPars.Key, inpPars.Value);
            }
            string logFileName = "magiclink_" + clientName + "_" + methodName;
            if (s.LogWithDatetimeEnabled == true)
            {
                logFileName += "_" + DateTime.Now.ToString("dd_MM_yyyy_HH_mm");
            }
            logFileName += ".log";
            Log("log", logFileName, logData);
        }

        public static void LogServiceException(Exception ex)
        {
            JsonSettings s = JsonSettings.Instance;
            if (s.LogExceptionEnabled == false)
            {
                return;
            }

            Dictionary<string, object> logData = new Dictionary<string, object>();
            logData["exceptionmessage"] = ex.Message;
            logData["exceptionstr"] = ex.Message;
            Log(null, "errors.log", logData);
        }
        */
    }
}
