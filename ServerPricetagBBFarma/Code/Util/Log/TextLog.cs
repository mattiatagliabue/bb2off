using System;
using System.Collections.Generic;
using System.IO;

namespace ServerPricetagBBFarma.Code.Util
{
    public class TextLog
    {
        public static void LogInfo(string logFile, string text)
        {
            using (StreamWriter w = File.AppendText(logFile))
            {
                var nowDate = DateTime.Now;
                w.Write(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:fff"));
                w.Write(" INFO: ");
                w.WriteLine(text);
                w.Close();
            }
        }

        public static void LogWarning(string logFile, string text)
        {
            using (StreamWriter w = File.AppendText(logFile))
            {
                var nowDate = DateTime.Now;
                w.Write(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:fff"));
                w.Write(" WARNING: ");
                w.WriteLine(text);
                w.Close();
            }
        }

        public static void LogError(string logFile, string text)
        {
            using (StreamWriter w = File.AppendText(logFile))
            {
                var nowDate = DateTime.Now;
                w.Write(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:fff"));
                w.Write(" ERROR: ");
                w.WriteLine(text);
                w.Close();
            }
        }

        public static void LogOther(string logFile, string tag, string text)
        {
            using (StreamWriter w = File.AppendText(logFile))
            {
                var nowDate = DateTime.Now;
                w.Write(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:fff"));
                w.Write(" ");
                w.Write(tag);
                w.Write(": ");
                w.WriteLine(text);
                w.Close();
            }
        }

        public static void LogOtherDict(string logFile, Dictionary<string, string> tagText)
        {
            using (StreamWriter w = File.AppendText(logFile))
            {
                var nowDate = DateTime.Now;
                foreach (var tt in tagText)
                {
                    w.Write(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:fff"));
                    w.Write(" ");
                    w.Write(tt.Key);
                    w.Write(": ");
                    w.WriteLine(tt.Value);
                }
                w.Close();
            }
        }
    }
}