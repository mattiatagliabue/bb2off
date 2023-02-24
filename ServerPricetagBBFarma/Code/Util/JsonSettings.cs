using Newtonsoft.Json;
using System;
using System.IO;

namespace ServerPricetagBBFarma.Code.Util
{
    public class JsonSettings
    {
        // Db Connection String
        public string DbString { get; set; }

        // Db Connection String (2)
        public string DbSqlConnection { get; set; }
        // Db Password
        public string Password { get; set; }

        // general log enabled
        public bool LogEnabled { get; set; }

        // request response log enabled
        public bool LogRequestresponseEnabled { get; set; }

        // magiclink log enabled
        public bool LogMagiclinkEnabled { get; set; }

        // sql log enabled
        public bool LogSqlEnabled { get; set; }

        public bool LogWithDatetimeEnabled { get; set; }

        // sql log enabled
        public bool LogExceptionEnabled { get; set; }

        public string IpStampante { get; set; }
        public bool CheckIP { get; set; }
        public int PortaStampante { get; set; }

        public string IpServerMago { get; set; }

        public string IpServerPriceTag { get; set; }
        public string IpRework { get; set; }
        public string WSWApp { get; set; }
        public string WebsitePort { get; set; }

        public string WebsitePath { get; set; }


        // setting filename
        private const string SETTINGS_FILENAME = "ServSettings.json";

        // sigleton
        private static JsonSettings instance = null;
        private static string baseDirPath = null;

        public static void SetBaseDirPath(string baseDirPath)
        {
            JsonSettings.baseDirPath = baseDirPath;
        }

        public static JsonSettings Instance
        {
            get
            {
                if (instance == null)
                {
                    if (baseDirPath == null)
                    {
                        throw new InvalidOperationException("baseDirPath not set");
                    }

                    instance = new JsonSettings();

                    // if file not exist do nothing
                    if (File.Exists(baseDirPath + SETTINGS_FILENAME) == false)
                    {
                        Console.WriteLine(baseDirPath + SETTINGS_FILENAME + " not exist!");
                    }

                    // try load file
                    using (StreamReader reader = new StreamReader(baseDirPath + SETTINGS_FILENAME))
                    {
                        JsonSettings.instance = JsonConvert.DeserializeObject<JsonSettings>(reader.ReadToEnd());
                        reader.Close();
                    }
                }
                return instance;
            }
        }
    }
}