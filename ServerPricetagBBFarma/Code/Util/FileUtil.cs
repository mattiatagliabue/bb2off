using System;
using System.IO;

namespace ServerPricetagBBFarma.Code.Util
{
    public class FileUtil
    {
        public static string GetOrCreateDirPath(string basePath, string dirName)
        {
            string dirPath = Path.Combine(basePath, dirName);
            if (Directory.Exists(dirPath) == false)
            {
                Directory.CreateDirectory(dirPath);
            }
            return dirPath;
        }
        public static byte[] FromHex(string hex)
        {
            hex = hex.Replace("-", "");
            byte[] raw = new byte[hex.Length / 2];
            for (int i = 0; i < raw.Length; i++)
            {
                raw[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
            }
            return raw;
        }
    }
}