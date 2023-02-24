using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace SDCommon.Barcode
{
    public class IFADatamatrixParser
    {
        public IFADatamatrix TryParseIFADatamatrix(string rawBarcodeData)
        {
            // if null return
            if (rawBarcodeData == null)
            {
                return null;
            }

            // if empty return
            if (rawBarcodeData.Length == 0)
            {
                return null;
            }

            // remove start pattern if any
            HashSet<string> symbologyIdentifier = new HashSet<string>();
            symbologyIdentifier.Add("]C1");
            symbologyIdentifier.Add("]e0");
            symbologyIdentifier.Add("]d2");
            symbologyIdentifier.Add("]Q3");
            symbologyIdentifier.Add("[)>RS06GS");
            symbologyIdentifier.Add("[)>\u001e06\u001d");
            symbologyIdentifier.Add("[)>06\u001d");
            symbologyIdentifier.Add("[)>06 ");
            symbologyIdentifier.Add("[)>");

            bool symIdRemoved = false;
            foreach (string symId in symbologyIdentifier)
            {
                if (rawBarcodeData.StartsWith(symId))
                {
                    if (symIdRemoved == false)
                    {
                        rawBarcodeData = rawBarcodeData.Substring(symId.Length);
                        symIdRemoved = true;
                    }
                }
            }

            // remove end pattern if any
            if (rawBarcodeData.IndexOf("RSEOT") > 0)
            {
                rawBarcodeData = rawBarcodeData.Substring(0, rawBarcodeData.IndexOf("RSEOT"));
            }
            if (rawBarcodeData.IndexOf("\u001E\u0004") > 0)
            {
                rawBarcodeData = rawBarcodeData.Substring(0, rawBarcodeData.IndexOf("\u001E\u0004"));
            }

            // try parse datamatrix
            IFADatamatrix ifadm = ParseIFADatamatrix(rawBarcodeData);

            // check that required data is present
            if (ifadm != null)
            {
                if ((ifadm.gtin.Length > 0 || ifadm.ppn.Length > 0)
                        && ifadm.batch.Length > 0
                        && ifadm.serialNumber.Length > 0
                        && (ifadm.expirationDate.Length > 0 || ifadm.productionDate.Length > 0))
                {
                    return ifadm;
                }
            }

            return null;
        }

        public IFADatamatrix TryParseIFADatamatrixNoRequiredId(string rawBarcodeData)
        {
            // if null return
            if (rawBarcodeData == null)
            {
                return null;
            }

            // if empty return
            if (rawBarcodeData.Length == 0)
            {
                return null;
            }

            // remove start pattern if any
            HashSet<string> symbologyIdentifier = new HashSet<string>();
            symbologyIdentifier.Add("]C1");
            symbologyIdentifier.Add("]e0");
            symbologyIdentifier.Add("]d2");
            symbologyIdentifier.Add("]Q3");
            symbologyIdentifier.Add("[)>RS06GS");
            symbologyIdentifier.Add("[)>\u001e06\u001d");

            bool symIdRemoved = false;
            foreach (string symId in symbologyIdentifier)
            {
                if (rawBarcodeData.StartsWith(symId))
                {
                    if (symIdRemoved == false)
                    {
                        rawBarcodeData = rawBarcodeData.Substring(symId.Length);
                        symIdRemoved = true;
                    }
                }
            }

            // remove end pattern if any
            if (rawBarcodeData.IndexOf("RSEOT") > 0)
            {
                rawBarcodeData = rawBarcodeData.Substring(0, rawBarcodeData.IndexOf("RSEOT"));
            }
            if (rawBarcodeData.IndexOf("\u001E\u0004") > 0)
            {
                rawBarcodeData = rawBarcodeData.Substring(0, rawBarcodeData.IndexOf("\u001E\u0004"));
            }

            // try parse datamatrix
            IFADatamatrix ifadm = ParseIFADatamatrix(rawBarcodeData);

            // check that required data is present
            if (ifadm != null)
            {
                if (ifadm.gtin.Length > 0
                    || ifadm.ppn.Length > 0
                    || ifadm.batch.Length > 0
                    || ifadm.serialNumber.Length > 0
                    || ifadm.expirationDate.Length > 0
                    || ifadm.productionDate.Length > 0
                    || ifadm.price.Length > 0)
                {
                    return ifadm;
                }
            }

            return null;
        }

        public IFADatamatrix ParseIFADatamatrix(string rawBarcodeData)
        {
            // try get datamatrix parts
            Dictionary<string, string> data = ParseIFADatamatrixInt(rawBarcodeData.ToCharArray(), 0);

            // return if no parts
            if (data == null)
            {
                return null;
            }

            // fill dm fields
            IFADatamatrix dm = new IFADatamatrix();
            dm.completeBarcode = rawBarcodeData;
            if (data.ContainsKey("9N"))
            {
                dm.ppn = data["9N"];
            }
            if (data.ContainsKey("8P"))
            {
                dm.gtin = data["8P"];
            }
            if (data.ContainsKey("1T"))
            {
                dm.batch = data["1T"];
            }
            if (data.ContainsKey("16D"))
            {
                dm.productionDate = data["16D"];
            }
            if (data.ContainsKey("27Q"))
            {
                dm.price = data["27Q"];
            }
            if (data.ContainsKey("D"))
            {
                dm.expirationDate = data["D"];
            }
            if (data.ContainsKey("S"))
            {
                dm.serialNumber = data["S"];
            }

            return dm;
        }

        private Dictionary<string, string> ParseIFADatamatrixInt(char[] cs, int i)
        {
            Dictionary<string, string> data = new Dictionary<string, string>();

            while (i < cs.Length)
            {
                int ctrLen = 3;
                int appIdLen = 0;
                if ((i + ctrLen) > cs.Length)
                {
                    ctrLen = cs.Length - i;
                }
                string ctr = new string(cs, i, ctrLen);
                if (ctr.StartsWith("16D") || ctr.StartsWith("27Q"))
                {
                    appIdLen = 3;
                }
                else
                {
                    if (ctr.StartsWith("9N") || ctr.StartsWith("1T") || ctr.StartsWith("8P"))
                    {
                        appIdLen = 2;
                    }
                    else if (ctr.StartsWith("D") || ctr.StartsWith("S"))
                    {
                        appIdLen = 1;
                    }
                }
                if (appIdLen == 0)
                {
                    return null;
                }

                // variable size with separator
                int separatorPos = i + appIdLen;
                while (separatorPos < cs.Length && (cs[separatorPos] != ((char)29) && cs[separatorPos] != ' '))
                {
                    separatorPos++;
                }
                if (separatorPos == (i + appIdLen))
                {
                    return null;
                }
                string appId = new string(cs, i, appIdLen);
                string dataStr = new string(cs, i + appIdLen, (separatorPos - i - appIdLen));

                if (data.ContainsKey(appId) == false)
                {
                    data.Add(appId, dataStr);
                }
                i = separatorPos + 1;
            }

            return data;
        }
    }
}